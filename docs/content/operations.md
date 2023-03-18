# Operations

Here is some information on actually running Kafka as a production
system based on usage and experience at LinkedIn. Please send us any
additional tips you know of.

## 6.1 Basic Kafka Operations {#basic_ops .anchor-link}

This section will review the most common operations you will perform on
your Kafka cluster. All of the tools reviewed in this section are
available under the `bin/` directory of the Kafka distribution and each
tool will print details on all possible commandline options if it is run
with no arguments.

### Adding and removing topics {#basic_ops_add_topic .anchor-link}

You have the option of either adding topics manually or having them be
created automatically when data is first published to a non-existent
topic. If topics are auto-created then you may want to tune the default
[topic configurations](../configuration#topicconfigs) used for auto-created topics.

Topics are added and modified using the topic tool:

```bash
> bin/kafka-topics.sh --bootstrap-server broker_host:port --create --topic my_topic_name \
        --partitions 20 --replication-factor 3 --config x=y
```

The replication factor controls how many servers will replicate each
message that is written. If you have a replication factor of 3 then up
to 2 servers can fail before you will lose access to your data. We
recommend you use a replication factor of 2 or 3 so that you can
transparently bounce machines without interrupting data consumption.

The partition count controls how many logs the topic will be sharded
into. There are several impacts of the partition count. First each
partition must fit entirely on a single server. So if you have 20
partitions the full data set (and read and write load) will be handled
by no more than 20 servers (not counting replicas). Finally the
partition count impacts the maximum parallelism of your consumers. This
is discussed in greater detail in the [concepts section](../design#theconsumer).

Each sharded partition log is placed into its own folder under the Kafka
log directory. The name of such folders consists of the topic name,
appended by a dash (-) and the partition id. Since a typical folder name
can not be over 255 characters long, there will be a limitation on the
length of topic names. We assume the number of partitions will not ever
be above 100,000. Therefore, topic names cannot be longer than 249
characters. This leaves just enough room in the folder name for a dash
and a potentially 5 digit long partition id.

The configurations added on the command line override the default
settings the server has for things like the length of time data should
be retained. The complete set of per-topic configurations is documented
[here](../configuration#topicconfigs).

### Modifying topics {#basic_ops_modify_topic .anchor-link}

You can change the configuration or partitioning of a topic using the
same topic tool.

To add partitions you can do

```shell {linenos=false}
> bin/kafka-topics.sh --bootstrap-server broker_host:port --alter --topic my_topic_name \
        --partitions 40
```

Be aware that one use case for partitions is to semantically partition
data, and adding partitions doesn\'t change the partitioning of existing
data so this may disturb consumers if they rely on that partition. That
is if data is partitioned by `hash(key) % number_of_partitions` then
this partitioning will potentially be shuffled by adding partitions but
Kafka will not attempt to automatically redistribute data in any way.

To add configs:

```shell {linenos=false}
> bin/kafka-configs.sh --bootstrap-server broker_host:port --entity-type topics --entity-name my_topic_name --alter --add-config x=y
```

To remove a config:

```shell {linenos=false}
> bin/kafka-configs.sh --bootstrap-server broker_host:port --entity-type topics --entity-name my_topic_name --alter --delete-config x
```

And finally deleting a topic:

```shell {linenos=false}
> bin/kafka-topics.sh --bootstrap-server broker_host:port --delete --topic my_topic_name
```

Kafka does not currently support reducing the number of partitions for a
topic.

Instructions for changing the replication factor of a topic can be found
[here](#basic_ops_increase_replication_factor).

### Graceful shutdown {#basic_ops_restarting .anchor-link}

The Kafka cluster will automatically detect any broker shutdown or
failure and elect new leaders for the partitions on that machine. This
will occur whether a server fails or it is brought down intentionally
for maintenance or configuration changes. For the latter cases Kafka
supports a more graceful mechanism for stopping a server than just
killing it. When a server is stopped gracefully it has two optimizations
it will take advantage of:

1.  It will sync all its logs to disk to avoid needing to do any log
    recovery when it restarts (i.e. validating the checksum for all
    messages in the tail of the log). Log recovery takes time so this
    speeds up intentional restarts.
2.  It will migrate any partitions the server is the leader for to other
    replicas prior to shutting down. This will make the leadership
    transfer faster and minimize the time each partition is unavailable
    to a few milliseconds.

Syncing the logs will happen automatically whenever the server is
stopped other than by a hard kill, but the controlled leadership
migration requires using a special setting:

```java-properties
controlled.shutdown.enable=true
```

Note that controlled shutdown will only succeed if *all* the partitions
hosted on the broker have replicas (i.e. the replication factor is
greater than 1 *and* at least one of these replicas is alive). This is
generally what you want since shutting down the last replica would make
that topic partition unavailable.

### Balancing leadership {#basic_ops_leader_balancing .anchor-link}

Whenever a broker stops or crashes, leadership for that broker\'s
partitions transfers to other replicas. When the broker is restarted it
will only be a follower for all its partitions, meaning it will not be
used for client reads and writes.

To avoid this imbalance, Kafka has a notion of preferred replicas. If
the list of replicas for a partition is 1,5,9 then node 1 is preferred
as the leader to either node 5 or 9 because it is earlier in the replica
list. By default the Kafka cluster will try to restore leadership to the
preferred replicas. This behaviour is configured with:

```java-properties
auto.leader.rebalance.enable=true
```

You can also set this to false, but you will then need to manually
restore leadership to the restored replicas by running the command:

```shell {linenos=false}
> bin/kafka-leader-election.sh --bootstrap-server broker_host:port --election-type preferred --all-topic-partitions
```

### Balancing Replicas Across Racks {#basic_ops_racks .anchor-link}

The rack awareness feature spreads replicas of the same partition across
different racks. This extends the guarantees Kafka provides for
broker-failure to cover rack-failure, limiting the risk of data loss
should all the brokers on a rack fail at once. The feature can also be
applied to other broker groupings such as availability zones in EC2.

You can specify that a broker belongs to a particular rack by adding a
property to the broker config:

```java-properties language-text
broker.rack=my-rack-id
```

When a topic is [created](#basic_ops_add_topic),
[modified](#basic_ops_modify_topic) or replicas are
[redistributed](#basic_ops_cluster_expansion), the rack constraint will
be honoured, ensuring replicas span as many racks as they can (a
partition will span min(#racks, replication-factor) different racks).

The algorithm used to assign replicas to brokers ensures that the number
of leaders per broker will be constant, regardless of how brokers are
distributed across racks. This ensures balanced throughput.

However if racks are assigned different numbers of brokers, the
assignment of replicas will not be even. Racks with fewer brokers will
get more replicas, meaning they will use more storage and put more
resources into replication. Hence it is sensible to configure an equal
number of brokers per rack.

### Mirroring data between clusters & Geo-replication {#basic_ops_mirror_maker .anchor-link}

Kafka administrators can define data flows that cross the boundaries of
individual Kafka clusters, data centers, or geographical regions. Please
refer to the section on [Geo-Replication](#georeplication) for further
information.

### Checking consumer position {#basic_ops_consumer_lag .anchor-link}

Sometimes it\'s useful to see the position of your consumers. We have a
tool that will show the position of all consumers in a consumer group as
well as how far behind the end of the log they are. To run this tool on
a consumer group named *my-group* consuming a topic named *my-topic*
would look like this:

```shell {linenos=false}
> bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group

  TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
  my-topic                       0          2               4               2          consumer-1-029af89c-873c-4751-a720-cefd41a669d6   /127.0.0.1                     consumer-1
  my-topic                       1          2               3               1          consumer-1-029af89c-873c-4751-a720-cefd41a669d6   /127.0.0.1                     consumer-1
  my-topic                       2          2               3               1          consumer-2-42c1abd4-e3b2-425d-a8bb-e1ea49b29bb2   /127.0.0.1                     consumer-2
```

### Managing Consumer Groups {#basic_ops_consumer_group .anchor-link}

With the ConsumerGroupCommand tool, we can list, describe, or delete the
consumer groups. The consumer group can be deleted manually, or
automatically when the last committed offset for that group expires.
Manual deletion works only if the group does not have any active
members. For example, to list all consumer groups across all topics:

```shell {linenos=false}
> bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

  test-consumer-group
```

To view offsets, as mentioned earlier, we \"describe\" the consumer
group like this:

```shell {linenos=false}
> bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group

  TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                    HOST            CLIENT-ID
  topic3          0          241019          395308          154289          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
  topic2          1          520678          803288          282610          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
  topic3          1          241018          398817          157799          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
  topic1          0          854144          855809          1665            consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1
  topic2          0          460537          803290          342753          consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1
  topic3          2          243655          398812          155157          consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4
```

There are a number of additional \"describe\" options that can be used
to provide more detailed information about a consumer group:

-   `--members`: This option provides the list of all active members in
    the consumer group.

    ```shell {linenos=false}
        > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members

          CONSUMER-ID                                    HOST            CLIENT-ID       #PARTITIONS
          consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1       2
          consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4       1
          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2       3
          consumer3-ecea43e4-1f01-479f-8349-f9130b75d8ee /127.0.0.1      consumer3       0
    ```

-   `--members --verbose`: On top of the information reported by the
    \"\--members\" options above, this option also provides the
    partitions assigned to each member.

    ```shell {linenos=false}
        > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members --verbose

          CONSUMER-ID                                    HOST            CLIENT-ID       #PARTITIONS     ASSIGNMENT
          consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1       2               topic1(0), topic2(0)
          consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4       1               topic3(2)
          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2       3               topic2(1), topic3(0,1)
          consumer3-ecea43e4-1f01-479f-8349-f9130b75d8ee /127.0.0.1      consumer3       0               -
    ```

-   `--offsets`: This is the default describe option and provides the
    same output as the \"\--describe\" option.

-   `--state`: This option provides useful group-level information.

    ```shell {linenos=false}
        > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --state

          COORDINATOR (ID)          ASSIGNMENT-STRATEGY       STATE                #MEMBERS
          localhost:9092 (0)        range                     Stable               4
    ```

To manually delete one or multiple consumer groups, the \"\--delete\"
option can be used:

```shell {linenos=false}
> bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group my-group --group my-other-group

  Deletion of requested consumer groups ('my-group', 'my-other-group') was successful.
```

To reset offsets of a consumer group, \"\--reset-offsets\" option can be
used. This option supports one consumer group at the time. It requires
defining following scopes: \--all-topics or \--topic. One scope must be
selected, unless you use \'\--from-file\' scenario. Also, first make
sure that the consumer instances are inactive. See
[KIP-122](https://cwiki.apache.org/confluence/display/KAFKA/KIP-122%3A+Add+Reset+Consumer+Group+Offsets+tooling)
for more details.

It has 3 execution options:

-   (default) to display which offsets to reset.
-   `--execute` : to execute \--reset-offsets process.
-   `--export` : to export the results to a CSV format.

`--reset-offsets` also has following scenarios to choose from (at least one scenario must be selected):

-   `--to-datetime <String: datetime>` : Reset offsets to offsets from
    datetime. Format: \'YYYY-MM-DDTHH:mm:SS.sss\'
-   `--to-earliest` : Reset offsets to earliest offset.
-   `--to-latest` : Reset offsets to latest offset.
-   `--shift-by <Long: number-of-offsets>` : Reset offsets shifting
    current offset by \'n\', where \'n\' can be positive or negative.
-   `--from-file` : Reset offsets to values defined in CSV file.
-   `--to-current` : Resets offsets to current offset.
-   `--by-duration <String: duration>` : Reset offsets to offset by
    duration from current timestamp. Format: \'PnDTnHnMnS\'
-   `--to-offset` : Reset offsets to a specific offset.

Please note, that out of range offsets will be adjusted to available
offset end. For example, if offset end is at 10 and offset shift request
is of 15, then, offset at 10 will actually be selected.

For example, to reset offsets of a consumer group to the latest offset:

```shell {linenos=false}
> bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group consumergroup1 --topic topic1 --to-latest

  TOPIC                          PARTITION  NEW-OFFSET
  topic1                         0          0
```

If you are using the old high-level consumer and storing the group
metadata in ZooKeeper (i.e. `offsets.storage=zookeeper`), pass
`--zookeeper` instead of `--bootstrap-server`:

```shell {linenos=false}
> bin/kafka-consumer-groups.sh --zookeeper localhost:2181 --list
```

### Expanding your cluster {#basic_ops_cluster_expansion .anchor-link}

Adding servers to a Kafka cluster is easy, just assign them a unique
broker id and start up Kafka on your new servers. However these new
servers will not automatically be assigned any data partitions, so
unless partitions are moved to them they won\'t be doing any work until
new topics are created. So usually when you add machines to your cluster
you will want to migrate some existing data to these machines.

The process of migrating data is manually initiated but fully automated.
Under the covers what happens is that Kafka will add the new server as a
follower of the partition it is migrating and allow it to fully
replicate the existing data in that partition. When the new server has
fully replicated the contents of this partition and joined the in-sync
replica one of the existing replicas will delete their partition\'s
data.

The partition reassignment tool can be used to move partitions across
brokers. An ideal partition distribution would ensure even data load and
partition sizes across all brokers. The partition reassignment tool does
not have the capability to automatically study the data distribution in
a Kafka cluster and move partitions around to attain an even load
distribution. As such, the admin has to figure out which topics or
partitions should be moved around.

The partition reassignment tool can run in 3 mutually exclusive modes:

-   `--generate`: In this mode, given a list of topics and a list of
    brokers, the tool generates a candidate reassignment to move all
    partitions of the specified topics to the new brokers. This option
    merely provides a convenient way to generate a partition
    reassignment plan given a list of topics and target brokers.
-   `--execute`: In this mode, the tool kicks off the reassignment of
    partitions based on the user provided reassignment plan. (using the
    \--reassignment-json-file option). This can either be a custom
    reassignment plan hand crafted by the admin or provided by using the
    \--generate option
-   `--verify`: In this mode, the tool verifies the status of the
    reassignment for all partitions listed during the last \--execute.
    The status can be either of successfully completed, failed or in
    progress

#### Automatically migrating data to new machines {#basic_ops_automigrate .anchor-link}

The partition reassignment tool can be used to move some topics off of
the current set of brokers to the newly added brokers. This is typically
useful while expanding an existing cluster since it is easier to move
entire topics to the new set of brokers, than moving one partition at a
time. When used to do this, the user should provide a list of topics
that should be moved to the new set of brokers and a target list of new
brokers. The tool then evenly distributes all partitions for the given
list of topics across the new set of brokers. During this move, the
replication factor of the topic is kept constant. Effectively the
replicas for all partitions for the input list of topics are moved from
the old set of brokers to the newly added brokers.

For instance, the following example will move all partitions for topics
foo1,foo2 to the new set of brokers 5,6. At the end of this move, all
partitions for topics foo1 and foo2 will *only* exist on brokers 5,6.

Since the tool accepts the input list of topics as a json file, you
first need to identify the topics you want to move and create the json
file as follows:

```shell {linenos=false}
> cat topics-to-move.json
  {"topics": [{"topic": "foo1"},
              {"topic": "foo2"}],
  "version":1
  }
```

Once the json file is ready, use the partition reassignment tool to
generate a candidate assignment:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --topics-to-move-json-file topics-to-move.json --broker-list "5,6" --generate
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo1","partition":0,"replicas":[2,1]},
                {"topic":"foo1","partition":1,"replicas":[1,3]},
                {"topic":"foo1","partition":2,"replicas":[3,4]},
                {"topic":"foo2","partition":0,"replicas":[4,2]},
                {"topic":"foo2","partition":1,"replicas":[2,1]},
                {"topic":"foo2","partition":2,"replicas":[1,3]}]
  }

  Proposed partition reassignment configuration

  {"version":1,
  "partitions":[{"topic":"foo1","partition":0,"replicas":[6,5]},
                {"topic":"foo1","partition":1,"replicas":[5,6]},
                {"topic":"foo1","partition":2,"replicas":[6,5]},
                {"topic":"foo2","partition":0,"replicas":[5,6]},
                {"topic":"foo2","partition":1,"replicas":[6,5]},
                {"topic":"foo2","partition":2,"replicas":[5,6]}]
  }
```

The tool generates a candidate assignment that will move all partitions
from topics foo1,foo2 to brokers 5,6. Note, however, that at this point,
the partition movement has not started, it merely tells you the current
assignment and the proposed new assignment. The current assignment
should be saved in case you want to rollback to it. The new assignment
should be saved in a json file (e.g. expand-cluster-reassignment.json)
to be input to the tool with the \--execute option as follows:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --execute
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo1","partition":0,"replicas":[2,1]},
                {"topic":"foo1","partition":1,"replicas":[1,3]},
                {"topic":"foo1","partition":2,"replicas":[3,4]},
                {"topic":"foo2","partition":0,"replicas":[4,2]},
                {"topic":"foo2","partition":1,"replicas":[2,1]},
                {"topic":"foo2","partition":2,"replicas":[1,3]}]
  }

  Save this to use as the --reassignment-json-file option during rollback
  Successfully started partition reassignments for foo1-0,foo1-1,foo1-2,foo2-0,foo2-1,foo2-2
  
```

Finally, the \--verify option can be used with the tool to check the
status of the partition reassignment. Note that the same
expand-cluster-reassignment.json (used with the \--execute option)
should be used with the \--verify option:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --verify
  Status of partition reassignment:
  Reassignment of partition [foo1,0] is completed
  Reassignment of partition [foo1,1] is still in progress
  Reassignment of partition [foo1,2] is still in progress
  Reassignment of partition [foo2,0] is completed
  Reassignment of partition [foo2,1] is completed
  Reassignment of partition [foo2,2] is completed
```

#### Custom partition assignment and migration {#basic_ops_partitionassignment .anchor-link}

The partition reassignment tool can also be used to selectively move
replicas of a partition to a specific set of brokers. When used in this
manner, it is assumed that the user knows the reassignment plan and does
not require the tool to generate a candidate reassignment, effectively
skipping the \--generate step and moving straight to the \--execute step

For instance, the following example moves partition 0 of topic foo1 to
brokers 5,6 and partition 1 of topic foo2 to brokers 2,3:

The first step is to hand craft the custom reassignment plan in a json
file:

```shell {linenos=false}
> cat custom-reassignment.json
  {"version":1,"partitions":[{"topic":"foo1","partition":0,"replicas":[5,6]},{"topic":"foo2","partition":1,"replicas":[2,3]}]}
```

Then, use the json file with the \--execute option to start the
reassignment process:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --execute
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo1","partition":0,"replicas":[1,2]},
                {"topic":"foo2","partition":1,"replicas":[3,4]}]
  }

  Save this to use as the --reassignment-json-file option during rollback
  Successfully started partition reassignments for foo1-0,foo2-1
  
```

The \--verify option can be used with the tool to check the status of
the partition reassignment. Note that the same custom-reassignment.json
(used with the \--execute option) should be used with the \--verify
option:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --verify
  Status of partition reassignment:
  Reassignment of partition [foo1,0] is completed
  Reassignment of partition [foo2,1] is completed
```

### Decommissioning brokers {#basic_ops_decommissioning_brokers .anchor-link}

The partition reassignment tool does not have the ability to
automatically generate a reassignment plan for decommissioning brokers
yet. As such, the admin has to come up with a reassignment plan to move
the replica for all partitions hosted on the broker to be
decommissioned, to the rest of the brokers. This can be relatively
tedious as the reassignment needs to ensure that all the replicas are
not moved from the decommissioned broker to only one other broker. To
make this process effortless, we plan to add tooling support for
decommissioning brokers in the future.

### Increasing replication factor {#basic_ops_increase_replication_factor .anchor-link}

Increasing the replication factor of an existing partition is easy. Just
specify the extra replicas in the custom reassignment json file and use
it with the \--execute option to increase the replication factor of the
specified partitions.

For instance, the following example increases the replication factor of
partition 0 of topic foo from 1 to 3. Before increasing the replication
factor, the partition\'s only replica existed on broker 5. As part of
increasing the replication factor, we will add more replicas on brokers
6 and 7.

The first step is to hand craft the custom reassignment plan in a json
file:

```shell {linenos=false}
> cat increase-replication-factor.json
  {"version":1,
  "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}
```

Then, use the json file with the \--execute option to start the
reassignment process:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --execute
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo","partition":0,"replicas":[5]}]}

  Save this to use as the --reassignment-json-file option during rollback
  Successfully started partition reassignment for foo-0
```

The \--verify option can be used with the tool to check the status of
the partition reassignment. Note that the same
increase-replication-factor.json (used with the \--execute option)
should be used with the \--verify option:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --verify
  Status of partition reassignment:
  Reassignment of partition [foo,0] is completed
```

You can also verify the increase in replication factor with the
kafka-topics tool:

```shell {linenos=false}
> bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic foo --describe
  Topic:foo PartitionCount:1    ReplicationFactor:3 Configs:
    Topic: foo  Partition: 0    Leader: 5   Replicas: 5,6,7 Isr: 5,6,7
```

### Limiting Bandwidth Usage during Data Migration {#rep-throttle .anchor-link}

Kafka lets you apply a throttle to replication traffic, setting an upper
bound on the bandwidth used to move replicas from machine to machine.
This is useful when rebalancing a cluster, bootstrapping a new broker or
adding or removing brokers, as it limits the impact these data-intensive
operations will have on users.

There are two interfaces that can be used to engage a throttle. The
simplest, and safest, is to apply a throttle when invoking the
kafka-reassign-partitions.sh, but kafka-configs.sh can also be used to
view and alter the throttle values directly.

So for example, if you were to execute a rebalance, with the below
command, it would move partitions at no more than 50MB/s.

```shell {linenos=false}
$ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --execute --reassignment-json-file bigger-cluster.json --throttle 50000000
```

When you execute this script you will see the throttle engage:

```shell {linenos=false}
  The inter-broker throttle limit was set to 50000000 B/s
  Successfully started partition reassignment for foo1-0
```

Should you wish to alter the throttle, during a rebalance, say to
increase the throughput so it completes quicker, you can do this by
re-running the execute command with the \--additional option passing the
same reassignment-json-file:

```shell {linenos=false}
$ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092  --additional --execute --reassignment-json-file bigger-cluster.json --throttle 700000000
  The inter-broker throttle limit was set to 700000000 B/s
```

Once the rebalance completes the administrator can check the status of
the rebalance using the \--verify option. If the rebalance has
completed, the throttle will be removed via the \--verify command. It is
important that administrators remove the throttle in a timely manner
once rebalancing completes by running the command with the \--verify
option. Failure to do so could cause regular replication traffic to be
throttled.

When the \--verify option is executed, and the reassignment has
completed, the script will confirm that the throttle was removed:

```shell {linenos=false}
> bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092  --verify --reassignment-json-file bigger-cluster.json
  Status of partition reassignment:
  Reassignment of partition [my-topic,1] is completed
  Reassignment of partition [my-topic,0] is completed

  Clearing broker-level throttles on brokers 1,2,3
  Clearing topic-level throttles on topic my-topic
```

The administrator can also validate the assigned configs using the
kafka-configs.sh. There are two pairs of throttle configuration used to
manage the throttling process. First pair refers to the throttle value
itself. This is configured, at a broker level, using the dynamic
properties:

```shell {linenos=false}
    leader.replication.throttled.rate
    follower.replication.throttled.rate
```

Then there is the configuration pair of enumerated sets of throttled
replicas:

```shell {linenos=false}
    leader.replication.throttled.replicas
    follower.replication.throttled.replicas
```

Which are configured per topic.

All four config values are automatically assigned by
kafka-reassign-partitions.sh (discussed below).

To view the throttle limit configuration:

```shell {linenos=false}
> bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type brokers
  Configs for brokers '2' are leader.replication.throttled.rate=700000000,follower.replication.throttled.rate=700000000
  Configs for brokers '1' are leader.replication.throttled.rate=700000000,follower.replication.throttled.rate=700000000
```

This shows the throttle applied to both leader and follower side of the
replication protocol. By default both sides are assigned the same
throttled throughput value.

To view the list of throttled replicas:

```shell {linenos=false}
> bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type topics
  Configs for topic 'my-topic' are leader.replication.throttled.replicas=1:102,0:101,
      follower.replication.throttled.replicas=1:101,0:102
```

Here we see the leader throttle is applied to partition 1 on broker 102
and partition 0 on broker 101. Likewise the follower throttle is applied
to partition 1 on broker 101 and partition 0 on broker 102.

By default kafka-reassign-partitions.sh will apply the leader throttle
to all replicas that exist before the rebalance, any one of which might
be leader. It will apply the follower throttle to all move destinations.
So if there is a partition with replicas on brokers 101,102, being
reassigned to 102,103, a leader throttle, for that partition, would be
applied to 101,102 and a follower throttle would be applied to 103 only.

If required, you can also use the \--alter switch on kafka-configs.sh to
alter the throttle configurations manually.

#### Safe usage of throttled replication

Some care should be taken when using throttled replication. In
particular:

*(1) Throttle Removal:*

The throttle should be removed in a timely manner once reassignment
completes (by running kafka-reassign-partitions.sh \--verify).

*(2) Ensuring Progress:*

If the throttle is set too low, in comparison to the incoming write
rate, it is possible for replication to not make progress. This occurs
when:

    max(BytesInPerSec) > throttle

Where BytesInPerSec is the metric that monitors the write throughput of
producers into each broker.

The administrator can monitor whether replication is making progress,
during the rebalance, using the metric:

    kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=([-.\w]+),topic=([-.\w]+),partition=([0-9]+)

The lag should constantly decrease during replication. If the metric
does not decrease the administrator should increase the throttle
throughput as described above.

### Setting quotas {#quotas .anchor-link}

Quotas overrides and defaults may be configured at (user, client-id),
user or client-id levels as described [here](../design#design_quotas). By
default, clients receive an unlimited quota. It is possible to set
custom quotas for each (user, client-id), user or client-id group.

Configure custom quota for (user=user1, client-id=clientA):

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
  Updated config for entity: user-principal 'user1', client-id 'clientA'.
```

Configure custom quota for user=user1:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1
  Updated config for entity: user-principal 'user1'.
```

Configure custom quota for client-id=clientA:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-name clientA
  Updated config for entity: client-id 'clientA'.
```

It is possible to set default quotas for each (user, client-id), user or
client-id group by specifying *\--entity-default* option instead of
*\--entity-name*.

Configure default client-id quota for user=userA:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-default
  Updated config for entity: user-principal 'user1', default client-id.
```

Configure default quota for user:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-default
  Updated config for entity: default user-principal.
```

Configure default quota for client-id:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-default
  Updated config for entity: default client-id.
```

Here\'s how to describe the quota for a given (user, client-id):

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
  Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

Describe quota for a given user:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users --entity-name user1
  Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

Describe quota for a given client-id:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name clientA
  Configs for client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

If entity name is not specified, all entities of the specified type are
described. For example, describe all users:

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users
  Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
  Configs for default user-principal are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

Similarly for (user, client):

```shell {linenos=false}
> bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users --entity-type clients
  Configs for user-principal 'user1', default client-id are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
  Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

## 6.2 Datacenters {#datacenters .anchor-link}

Some deployments will need to manage a data pipeline that spans multiple
datacenters. Our recommended approach to this is to deploy a local Kafka
cluster in each datacenter, with application instances in each
datacenter interacting only with their local cluster and mirroring data
between clusters (see the documentation on
[Geo-Replication](#georeplication) for how to do this).

This deployment pattern allows datacenters to act as independent
entities and allows us to manage and tune inter-datacenter replication
centrally. This allows each facility to stand alone and operate even if
the inter-datacenter links are unavailable: when this occurs the
mirroring falls behind until the link is restored at which time it
catches up.

For applications that need a global view of all data you can use
mirroring to provide clusters which have aggregate data mirrored from
the local clusters in *all* datacenters. These aggregate clusters are
used for reads by applications that require the full data set.

This is not the only possible deployment pattern. It is possible to read
from or write to a remote Kafka cluster over the WAN, though obviously
this will add whatever latency is required to get the cluster.

Kafka naturally batches data in both the producer and consumer so it can
achieve high-throughput even over a high-latency connection. To allow
this though it may be necessary to increase the TCP socket buffer sizes
for the producer, consumer, and broker using the
`socket.send.buffer.bytes` and `socket.receive.buffer.bytes`
configurations. The appropriate way to set this is documented
[here](http://en.wikipedia.org/wiki/Bandwidth-delay_product).

It is generally *not* advisable to run a *single* Kafka cluster that
spans multiple datacenters over a high-latency link. This will incur
very high replication latency both for Kafka writes and ZooKeeper
writes, and neither Kafka nor ZooKeeper will remain available in all
locations if the network between locations is unavailable.

## 6.3 Geo-Replication (Cross-Cluster Data Mirroring) {#georeplication .anchor-link}

### Geo-Replication Overview {#georeplication-overview .anchor-link}

Kafka administrators can define data flows that cross the boundaries of
individual Kafka clusters, data centers, or geo-regions. Such event
streaming setups are often needed for organizational, technical, or
legal requirements. Common scenarios include:

-   Geo-replication
-   Disaster recovery
-   Feeding edge clusters into a central, aggregate cluster
-   Physical isolation of clusters (such as production vs. testing)
-   Cloud migration or hybrid cloud deployments
-   Legal and compliance requirements

Administrators can set up such inter-cluster data flows with Kafka\'s
MirrorMaker (version 2), a tool to replicate data between different
Kafka environments in a streaming manner. MirrorMaker is built on top of
the Kafka Connect framework and supports features such as:

-   Replicates topics (data plus configurations)
-   Replicates consumer groups including offsets to migrate applications
    between clusters
-   Replicates ACLs
-   Preserves partitioning
-   Automatically detects new topics and partitions
-   Provides a wide range of metrics, such as end-to-end replication
    latency across multiple data centers/clusters
-   Fault-tolerant and horizontally scalable operations

*Note: Geo-replication with MirrorMaker replicates data across Kafka
clusters. This inter-cluster replication is different from Kafka\'s
[intra-cluster replication](#replication), which replicates data within
the same Kafka cluster.*

### What Are Replication Flows](#georeplication-flows) {#georeplication-flows .anchor-link}

With MirrorMaker, Kafka administrators can replicate topics, topic
configurations, consumer groups and their offsets, and ACLs from one or
more source Kafka clusters to one or more target Kafka clusters, i.e.,
across cluster environments. In a nutshell, MirrorMaker uses Connectors
to consume from source clusters and produce to target clusters.

These directional flows from source to target clusters are called
replication flows. They are defined with the format
`{source_cluster}->{target_cluster}` in the MirrorMaker configuration
file as described later. Administrators can create complex replication
topologies based on these flows.

Here are some example patterns:

-   Active/Active high availability deployments: `A->B, B->A`
-   Active/Passive or Active/Standby high availability deployments:
    `A->B`
-   Aggregation (e.g., from many clusters to one): `A->K, B->K, C->K`
-   Fan-out (e.g., from one to many clusters): `K->A, K->B, K->C`
-   Forwarding: `A->B, B->C, C->D`

By default, a flow replicates all topics and consumer groups. However,
each replication flow can be configured independently. For instance, you
can define that only specific topics or consumer groups are replicated
from the source cluster to the target cluster.

Here is a first example on how to configure data replication from a
`primary` cluster to a `secondary` cluster (an active/passive setup):

```java-properties
# Basic settings
clusters = primary, secondary
primary.bootstrap.servers = broker3-primary:9092
secondary.bootstrap.servers = broker5-secondary:9092

# Define replication flows
primary->secondary.enabled = true
primary->secondary.topics = foobar-topic, quux-.*
```

### Configuring Geo-Replication {#georeplication-mirrormaker .anchor-link}

The following sections describe how to configure and run a dedicated
MirrorMaker cluster. If you want to run MirrorMaker within an existing
Kafka Connect cluster or other supported deployment setups, please refer
to [KIP-382: MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0)
and be aware that the names of configuration settings may vary between
deployment modes.

Beyond what\'s covered in the following sections, further examples and
information on configuration settings are available at:

-   [MirrorMakerConfig](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMakerConfig.java),
    [MirrorConnectorConfig](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorConnectorConfig.java)
-   [DefaultTopicFilter](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/DefaultTopicFilter.java)
    for topics,
    [DefaultGroupFilter](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/DefaultGroupFilter.java)
    for consumer groups
-   Example configuration settings in
    [connect-mirror-maker.properties](https://github.com/apache/kafka/blob/trunk/config/connect-mirror-maker.properties),
    [KIP-382: MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0)

#### Configuration File Syntax {#georeplication-config-syntax .anchor-link}

The MirrorMaker configuration file is typically named
`connect-mirror-maker.properties`. You can configure a variety of
components in this file:

-   MirrorMaker settings: global settings including cluster definitions
    (aliases), plus custom settings per replication flow
-   Kafka Connect and connector settings
-   Kafka producer, consumer, and admin client settings

Example: Define MirrorMaker settings (explained in more detail later).

```java-properties
# Global settings
clusters = us-west, us-east   # defines cluster aliases
us-west.bootstrap.servers = broker3-west:9092
us-east.bootstrap.servers = broker5-east:9092

topics = .*   # all topics to be replicated by default

# Specific replication flow settings (here: flow from us-west to us-east)
us-west->us-east.enabled = true
us-west->us.east.topics = foo.*, bar.*  # override the default above
```

MirrorMaker is based on the Kafka Connect framework. Any Kafka Connect,
source connector, and sink connector settings as described in the
[documentation chapter on Kafka Connect](#connectconfigs) can be used
directly in the MirrorMaker configuration, without having to change or
prefix the name of the configuration setting.

Example: Define custom Kafka Connect settings to be used by MirrorMaker.

```java-properties
# Setting Kafka Connect defaults for MirrorMaker
tasks.max = 5
```

Most of the default Kafka Connect settings work well for MirrorMaker
out-of-the-box, with the exception of `tasks.max`. In order to evenly
distribute the workload across more than one MirrorMaker process, it is
recommended to set `tasks.max` to at least `2` (preferably higher)
depending on the available hardware resources and the total number of
topic-partitions to be replicated.

You can further customize MirrorMaker\'s Kafka Connect settings *per
source or target cluster* (more precisely, you can specify Kafka Connect
worker-level configuration settings \"per connector\"). Use the format
of `{cluster}.{config_name}` in the MirrorMaker configuration file.

Example: Define custom connector settings for the `us-west` cluster.

```java-properties
# us-west custom settings
us-west.offset.storage.topic = my-mirrormaker-offsets
```

MirrorMaker internally uses the Kafka producer, consumer, and admin
clients. Custom settings for these clients are often needed. To override
the defaults, use the following format in the MirrorMaker configuration
file:

-   `{source}.consumer.{consumer_config_name}`
-   `{target}.producer.{producer_config_name}`
-   `{source_or_target}.admin.{admin_config_name}`

Example: Define custom producer, consumer, admin client settings.

```java-properties
# us-west cluster (from which to consume)
us-west.consumer.isolation.level = read_committed
us-west.admin.bootstrap.servers = broker57-primary:9092

# us-east cluster (to which to produce)
us-east.producer.compression.type = gzip
us-east.producer.buffer.memory = 32768
us-east.admin.bootstrap.servers = broker8-secondary:9092
```

#### Creating and Enabling Replication Flows {#georeplication-flow-create .anchor-link}

To define a replication flow, you must first define the respective
source and target Kafka clusters in the MirrorMaker configuration file.

-   `clusters` (required): comma-separated list of Kafka cluster
    \"aliases\"
-   `{clusterAlias}.bootstrap.servers` (required): connection
    information for the specific cluster; comma-separated list of
    \"bootstrap\" Kafka brokers

Example: Define two cluster aliases `primary` and `secondary`, including
their connection information.

```java-properties
clusters = primary, secondary
primary.bootstrap.servers = broker10-primary:9092,broker-11-primary:9092
secondary.bootstrap.servers = broker5-secondary:9092,broker6-secondary:9092
```

Secondly, you must explicitly enable individual replication flows with
`{source}->{target}.enabled = true` as needed. Remember that flows are
directional: if you need two-way (bidirectional) replication, you must
enable flows in both directions.

```java-properties
# Enable replication from primary to secondary
primary->secondary.enabled = true
```

By default, a replication flow will replicate all but a few special
topics and consumer groups from the source cluster to the target
cluster, and automatically detect any newly created topics and groups.
The names of replicated topics in the target cluster will be prefixed
with the name of the source cluster (see section further below). For
example, the topic `foo` in the source cluster `us-west` would be
replicated to a topic named `us-west.foo` in the target cluster
`us-east`.

The subsequent sections explain how to customize this basic setup
according to your needs.

#### Configuring Replication Flows {#georeplication-flow-configure .anchor-link}

The configuration of a replication flow is a combination of top-level
default settings (e.g., `topics`), on top of which flow-specific
settings, if any, are applied (e.g., `us-west->us-east.topics`). To
change the top-level defaults, add the respective top-level setting to
the MirrorMaker configuration file. To override the defaults for a
specific replication flow only, use the syntax format
`{source}->{target}.{config.name}`.

The most important settings are:

-   `topics`: list of topics or a regular expression that defines which
    topics in the source cluster to replicate (default: `topics = .*`)
-   `topics.exclude`: list of topics or a regular expression to
    subsequently exclude topics that were matched by the `topics`
    setting (default:
    `topics.exclude = .*[\-\.]internal, .*\.replica, __.*`)
-   `groups`: list of topics or regular expression that defines which
    consumer groups in the source cluster to replicate (default:
    `groups = .*`)
-   `groups.exclude`: list of topics or a regular expression to
    subsequently exclude consumer groups that were matched by the
    `groups` setting (default:
    `groups.exclude = console-consumer-.*, connect-.*, __.*`)
-   `{source}->{target}.enable`: set to `true` to enable the replication
    flow (default: `false`)

Example:

```shell {linenos=false}
# Custom top-level defaults that apply to all replication flows
topics = .*
groups = consumer-group1, consumer-group2

# Don't forget to enable a flow!
us-west->us-east.enabled = true

# Custom settings for specific replication flows
us-west->us-east.topics = foo.*
us-west->us-east.groups = bar.*
us-west->us-east.emit.heartbeats = false
```

Additional configuration settings are supported, some of which are
listed below. In most cases, you can leave these settings at their
default values. See
[MirrorMakerConfig](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMakerConfig.java)
and
[MirrorConnectorConfig](https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorConnectorConfig.java)
for further details.

-   `refresh.topics.enabled`: whether to check for new topics in the
    source cluster periodically (default: true)
-   `refresh.topics.interval.seconds`: frequency of checking for new
    topics in the source cluster; lower values than the default may lead
    to performance degradation (default: 600, every ten minutes)
-   `refresh.groups.enabled`: whether to check for new consumer groups
    in the source cluster periodically (default: true)
-   `refresh.groups.interval.seconds`: frequency of checking for new
    consumer groups in the source cluster; lower values than the default
    may lead to performance degradation (default: 600, every ten
    minutes)
-   `sync.topic.configs.enabled`: whether to replicate topic
    configurations from the source cluster (default: true)
-   `sync.topic.acls.enabled`: whether to sync ACLs from the source
    cluster (default: true)
-   `emit.heartbeats.enabled`: whether to emit heartbeats periodically
    (default: true)
-   `emit.heartbeats.interval.seconds`: frequency at which heartbeats
    are emitted (default: 1, every one seconds)
-   `heartbeats.topic.replication.factor`: replication factor of
    MirrorMaker\'s internal heartbeat topics (default: 3)
-   `emit.checkpoints.enabled`: whether to emit MirrorMaker\'s consumer
    offsets periodically (default: true)
-   `emit.checkpoints.interval.seconds`: frequency at which checkpoints
    are emitted (default: 60, every minute)
-   `checkpoints.topic.replication.factor`: replication factor of
    MirrorMaker\'s internal checkpoints topics (default: 3)
-   `sync.group.offsets.enabled`: whether to periodically write the
    translated offsets of replicated consumer groups (in the source
    cluster) to `__consumer_offsets` topic in target cluster, as long as
    no active consumers in that group are connected to the target
    cluster (default: false)
-   `sync.group.offsets.interval.seconds`: frequency at which consumer
    group offsets are synced (default: 60, every minute)
-   `offset-syncs.topic.replication.factor`: replication factor of
    MirrorMaker\'s internal offset-sync topics (default: 3)

#### Securing Replication Flows {#georeplication-flow-secure .anchor-link}

MirrorMaker supports the same 
[security settings as Kafka Connect](../configuration#connectconfigs), so please refer to the linked section for
further information.

Example: Encrypt communication between MirrorMaker and the `us-east`
cluster.

```java-properties
us-east.security.protocol=SSL
us-east.ssl.truststore.location=/path/to/truststore.jks
us-east.ssl.truststore.password=my-secret-password
us-east.ssl.keystore.location=/path/to/keystore.jks
us-east.ssl.keystore.password=my-secret-password
us-east.ssl.key.password=my-secret-password
```

#### Custom Naming of Replicated Topics in Target Clusters {#georeplication-topic-naming .anchor-link}

Replicated topics in a target cluster---sometimes called *remote*
topics---are renamed according to a replication policy. MirrorMaker uses
this policy to ensure that events (aka records, messages) from different
clusters are not written to the same topic-partition. By default as per
[DefaultReplicationPolicy](https://github.com/apache/kafka/blob/trunk/connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/DefaultReplicationPolicy.java),
the names of replicated topics in the target clusters have the format
`{source}.{source_topic_name}`:

```
us-west         us-east
=========       =================
                bar-topic
foo-topic  -->  us-west.foo-topic
```

You can customize the separator (default: `.`) with the
`replication.policy.separator` setting:

```java-properties
# Defining a custom separator
us-west->us-east.replication.policy.separator = _
```

If you need further control over how replicated topics are named, you
can implement a custom `ReplicationPolicy` and override
`replication.policy.class` (default is `DefaultReplicationPolicy`) in
the MirrorMaker configuration.

#### Preventing Configuration Conflicts {#georeplication-config-conflicts .anchor-link}

MirrorMaker processes share configuration via their target Kafka
clusters. This behavior may cause conflicts when configurations differ
among MirrorMaker processes that operate against the same target
cluster.

For example, the following two MirrorMaker processes would be racy:

```shell {linenos=false}
# Configuration of process 1
A->B.enabled = true
A->B.topics = foo

# Configuration of process 2
A->B.enabled = true
A->B.topics = bar
```

In this case, the two processes will share configuration via cluster
`B`, which causes a conflict. Depending on which of the two processes is
the elected \"leader\", the result will be that either the topic `foo`
or the topic `bar` is replicated, but not both.

It is therefore important to keep the MirrorMaker configration
consistent across replication flows to the same target cluster. This can
be achieved, for example, through automation tooling or by using a
single, shared MirrorMaker configuration file for your entire
organization.

#### Best Practice: Consume from Remote, Produce to Local {#georeplication-best-practice .anchor-link}

To minimize latency (\"producer lag\"), it is recommended to locate
MirrorMaker processes as close as possible to their target clusters,
i.e., the clusters that it produces data to. That\'s because Kafka
producers typically struggle more with unreliable or high-latency
network connections than Kafka consumers.

```
First DC          Second DC
==========        =========================
primary --------- MirrorMaker --> secondary
(remote)                           (local)
```

To run such a \"consume from remote, produce to local\" setup, run the
MirrorMaker processes close to and preferably in the same location as
the target clusters, and explicitly set these \"local\" clusters in the
`--clusters` command line parameter (blank-separated list of cluster
aliases):

```shell {linenos=false}
# Run in secondary's data center, reading from the remote `primary` cluster
$ ./bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters secondary
```

The `--clusters secondary` tells the MirrorMaker process that the given
cluster(s) are nearby, and prevents it from replicating data or sending
configuration to clusters at other, remote locations.

#### Example: Active/Passive High Availability Deployment {#georeplication-example-active-passive .anchor-link}

The following example shows the basic settings to replicate topics from
a primary to a secondary Kafka environment, but not from the secondary
back to the primary. Please be aware that most production setups will
need further configuration, such as security settings.

```shell {linenos=false}
# Unidirectional flow (one-way) from primary to secondary cluster
primary.bootstrap.servers = broker1-primary:9092
secondary.bootstrap.servers = broker2-secondary:9092

primary->secondary.enabled = true
secondary->primary.enabled = false

primary->secondary.topics = foo.*  # only replicate some topics
```

#### Example: Active/Active High Availability Deployment {#georeplication-example-active-active .anchor-link}

The following example shows the basic settings to replicate topics
between two clusters in both ways. Please be aware that most production
setups will need further configuration, such as security settings.

```shell {linenos=false}
# Bidirectional flow (two-way) between us-west and us-east clusters
clusters = us-west, us-east
us-west.bootstrap.servers = broker1-west:9092,broker2-west:9092
Us-east.bootstrap.servers = broker3-east:9092,broker4-east:9092

us-west->us-east.enabled = true
us-east->us-west.enabled = true
```

*Note on preventing replication \"loops\" (where topics will be
originally replicated from A to B, then the replicated topics will be
replicated yet again from B to A, and so forth)*: As long as you define
the above flows in the same MirrorMaker configuration file, you do not
need to explicitly add `topics.exclude` settings to prevent replication
loops between the two clusters.

#### Example: Multi-Cluster Geo-Replication {#georeplication-example-multi-cluster .anchor-link}

Let\'s put all the information from the previous sections together in a
larger example. Imagine there are three data centers (west, east,
north), with two Kafka clusters in each data center (e.g., `west-1`,
`west-2`). The example in this section shows how to configure
MirrorMaker (1) for Active/Active replication within each data center,
as well as (2) for Cross Data Center Replication (XDCR).

First, define the source and target clusters along with their
replication flows in the configuration:

```shell {linenos=false}
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
```

Then, in each data center, launch one or more MirrorMaker as follows:

```shell {linenos=false}
# In West DC:
$ ./bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters west-1 west-2

# In East DC:
$ ./bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters east-1 east-2

# In North DC:
$ ./bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters north-1 north-2
```

With this configuration, records produced to any cluster will be
replicated within the data center, as well as across to other data
centers. By providing the `--clusters` parameter, we ensure that each
MirrorMaker process produces data to nearby clusters only.

*Note:* The `--clusters` parameter is, technically, not required here.
MirrorMaker will work fine without it. However, throughput may suffer
from \"producer lag\" between data centers, and you may incur
unnecessary data transfer costs.

### Starting Geo-Replication {#georeplication-starting .anchor-link}

You can run as few or as many MirrorMaker processes (think: nodes,
servers) as needed. Because MirrorMaker is based on Kafka Connect,
MirrorMaker processes that are configured to replicate the same Kafka
clusters run in a distributed setup: They will find each other, share
configuration (see section below), load balance their work, and so on.
If, for example, you want to increase the throughput of replication
flows, one option is to run additional MirrorMaker processes in
parallel.

To start a MirrorMaker process, run the command:

```shell {linenos=false}
$ ./bin/connect-mirror-maker.sh connect-mirror-maker.properties
```

After startup, it may take a few minutes until a MirrorMaker process
first begins to replicate data.

Optionally, as described previously, you can set the parameter
`--clusters` to ensure that the MirrorMaker process produces data to
nearby clusters only.

```shell {linenos=false}
# Note: The cluster alias us-west must be defined in the configuration file
$ ./bin/connect-mirror-maker.sh connect-mirror-maker.properties \
            --clusters us-west
```

*Note when testing replication of consumer groups:* By default,
MirrorMaker does not replicate consumer groups created by the
`kafka-console-consumer.sh` tool, which you might use to test your
MirrorMaker setup on the command line. If you do want to replicate these
consumer groups as well, set the `groups.exclude` configuration
accordingly (default:
`groups.exclude = console-consumer-.*, connect-.*, __.*`). Remember to
update the configuration again once you completed your testing.

### Stopping Geo-Replication {#georeplication-stopping .anchor-link}

You can stop a running MirrorMaker process by sending a SIGTERM signal
with the command:

```shell {linenos=false}
$ kill <MirrorMaker pid>
```

### Applying Configuration Changes {#georeplication-apply-config-changes .anchor-link}

To make configuration changes take effect, the MirrorMaker process(es)
must be restarted.

### Monitoring Geo-Replication {#georeplication-monitoring .anchor-link}

It is recommended to monitor MirrorMaker processes to ensure all defined
replication flows are up and running correctly. MirrorMaker is built on
the Connect framework and inherits all of Connect\'s metrics, such
`source-record-poll-rate`. In addition, MirrorMaker produces its own
metrics under the `kafka.connect.mirror` metric group. Metrics are
tagged with the following properties:

-   `source`: alias of source cluster (e.g., `primary`)
-   `target`: alias of target cluster (e.g., `secondary`)
-   `topic`: replicated topic on target cluster
-   `partition`: partition being replicated

Metrics are tracked for each replicated topic. The source cluster can be
inferred from the topic name. For example, replicating `topic1` from
`primary->secondary` will yield metrics like:

-   `target=secondary`
-   `topic=primary.topic1`
-   `partition=1`

The following metrics are emitted:

```
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
```

These metrics do not differentiate between created-at and log-append
timestamps.

## 6.4 Multi-Tenancy {#multitenancy .anchor-link}

### Multi-Tenancy Overview {#multitenancy-overview .anchor-link}

As a highly scalable event streaming platform, Kafka is used by many
users as their central nervous system, connecting in real-time a wide
range of different systems and applications from various teams and lines
of businesses. Such multi-tenant cluster environments command proper
control and management to ensure the peaceful coexistence of these
different needs. This section highlights features and best practices to
set up such shared environments, which should help you operate clusters
that meet SLAs/OLAs and that minimize potential collateral damage caused
by \"noisy neighbors\".

Multi-tenancy is a many-sided subject, including but not limited to:

-   Creating user spaces for tenants (sometimes called namespaces)
-   Configuring topics with data retention policies and more
-   Securing topics and clusters with encryption, authentication, and
    authorization
-   Isolating tenants with quotas and rate limits
-   Monitoring and metering
-   Inter-cluster data sharing (cf. geo-replication)

### Creating User Spaces (Namespaces) For Tenants With Topic Naming {#multitenancy-topic-naming .anchor-link}

Kafka administrators operating a multi-tenant cluster typically need to
define user spaces for each tenant. For the purpose of this section,
\"user spaces\" are a collection of topics, which are grouped together
under the management of a single entity or user.

In Kafka, the main unit of data is the topic. Users can create and name
each topic. They can also delete them, but it is not possible to rename
a topic directly. Instead, to rename a topic, the user must create a new
topic, move the messages from the original topic to the new, and then
delete the original. With this in mind, it is recommended to define
logical spaces, based on an hierarchical topic naming structure. This
setup can then be combined with security features, such as prefixed
ACLs, to isolate different spaces and tenants, while also minimizing the
administrative overhead for securing the data in the cluster.

These logical user spaces can be grouped in different ways, and the
concrete choice depends on how your organization prefers to use your
Kafka clusters. The most common groupings are as follows.

*By team or organizational unit:* Here, the team is the main aggregator.
In an organization where teams are the main user of the Kafka
infrastructure, this might be the best grouping.

Example topic naming structure:

-   `<organization>.<team>.<dataset>.<event-name>`\
    (e.g., \"acme.infosec.telemetry.logins\")

*By project or product:* Here, a team manages more than one project.
Their credentials will be different for each project, so all the
controls and settings will always be project related.

Example topic naming structure:

-   `<project>.<product>.<event-name>`\
    (e.g., \"mobility.payments.suspicious\")

Certain information should normally not be put in a topic name, such as
information that is likely to change over time (e.g., the name of the
intended consumer) or that is a technical detail or metadata that is
available elsewhere (e.g., the topic\'s partition count and other
configuration settings).

To enforce a topic naming structure, several options are available:

-   Use [prefix ACLs](../security#security_authz) (cf.
    [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs))
    to enforce a common prefix for topic names. For example, team A may
    only be permitted to create topics whose names start with
    `payments.teamA.`.
-   Define a custom `CreateTopicPolicy` (cf.
    [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy)
    and the setting
    [create.topic.policy.class.name](../configuration#brokerconfigs_create.topic.policy.class.name))
    to enforce strict naming patterns. These policies provide the most
    flexibility and can cover complex patterns and rules to match an
    organization\'s needs.
-   Disable topic creation for normal users by denying it with an ACL,
    and then rely on an external process to create topics on behalf of
    users (e.g., scripting or your favorite automation toolkit).
-   It may also be useful to disable the Kafka feature to auto-create
    topics on demand by setting `auto.create.topics.enable=false` in the
    broker configuration. Note that you should not rely solely on this
    option.

### Configuring Topics: Data Retention And More {#multitenancy-topic-configs .anchor-link}

Kafka\'s configuration is very flexible due to its fine granularity, and
it supports a plethora of 
[per-topic configuration settings](../configuration#topicconfigs) to help administrators set up multi-tenant
clusters. For example, administrators often need to define data
retention policies to control how much and/or for how long data will be
stored in a topic, with settings such as
[retention.bytes](../configuration#topicconfigs_retention.bytes) (size) and
[retention.ms](../configuration#topicconfigs_#retention.ms) (time). This limits storage consumption
within the cluster, and helps complying with legal requirements such as
GDPR.

### Securing Clusters and Topics: Authentication, Authorization, Encryption {#multitenancy-security .anchor-link}

Because the documentation has a dedicated chapter on
[security](../security) that applies to any Kafka deployment, this section
focuses on additional considerations for multi-tenant environments.

Security settings for Kafka fall into three main categories, which are
similar to how administrators would secure other client-server data
systems, like relational databases and traditional messaging systems.

1.  **Encryption** of data transferred between Kafka brokers and Kafka
    clients, between brokers, between brokers and ZooKeeper nodes, and
    between brokers and other, optional tools.
2.  **Authentication** of connections from Kafka clients and
    applications to Kafka brokers, as well as connections from Kafka
    brokers to ZooKeeper nodes.
3.  **Authorization** of client operations such as creating, deleting,
    and altering the configuration of topics; writing events to or
    reading events from a topic; creating and deleting ACLs.
    Administrators can also define custom policies to put in place
    additional restrictions, such as a `CreateTopicPolicy` and
    `AlterConfigPolicy` (see
    [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy)
    and the settings
    [create.topic.policy.class.name](../configuration#brokerconfigs_create.topic.policy.class.name),
    [alter.config.policy.class.name](../configuration#brokerconfigs_alter.config.policy.class.name)).

When securing a multi-tenant Kafka environment, the most common
administrative task is the third category (authorization), i.e.,
managing the user/client permissions that grant or deny access to
certain topics and thus to the data stored by users within a cluster.
This task is performed predominantly through the [setting of access control lists (ACLs)](../security#security_authz). 
Here, administrators of multi-tenant environments in particular benefit from putting a
hierarchical topic naming structure in place as described in a previous
section, because they can conveniently control access to topics through
prefixed ACLs (`--resource-pattern-type Prefixed`). This significantly
minimizes the administrative overhead of securing topics in multi-tenant
environments: administrators can make their own trade-offs between
higher developer convenience (more lenient permissions, using fewer and
broader ACLs) vs. tighter security (more stringent permissions, using
more and narrower ACLs).

In the following example, user Alice---a new member of ACME
corporation\'s InfoSec team---is granted write permissions to all topics
whose names start with \"acme.infosec.\", such as
\"acme.infosec.telemetry.logins\" and \"acme.infosec.syslogs.events\".

```shell {linenos=false}
# Grant permissions to user Alice
$ bin/kafka-acls.sh \
    --bootstrap-server broker1:9092 \
    --add --allow-principal User:Alice \
    --producer \
    --resource-pattern-type prefixed --topic acme.infosec.
```

You can similarly use this approach to isolate different customers on
the same shared cluster.

### Isolating Tenants: Quotas, Rate Limiting, Throttling {#multitenancy-isolation .anchor-link}

Multi-tenant clusters should generally be configured with
[quotas](../design#design_quotas), which protect against users (tenants) eating
up too many cluster resources, such as when they attempt to write or
read very high volumes of data, or create requests to brokers at an
excessively high rate. This may cause network saturation, monopolize
broker resources, and impact other clients---all of which you want to
avoid in a shared environment.

**Client quotas:** Kafka supports different types of (per-user
principal) client quotas. Because a client\'s quotas apply irrespective
of which topics the client is writing to or reading from, they are a
convenient and effective tool to allocate resources in a multi-tenant
cluster. [Request rate quotas](../design#design_quotascpu), for example, help to
limit a user\'s impact on broker CPU usage by limiting the time a broker
spends on the [request handling path](../protocol) for that user,
after which throttling kicks in. In many situations, isolating users
with request rate quotas has a bigger impact in multi-tenant clusters
than setting incoming/outgoing network bandwidth quotas, because
excessive broker CPU usage for processing requests reduces the effective
bandwidth the broker can serve. Furthermore, administrators can also
define quotas on topic operations---such as create, delete, and
alter---to prevent Kafka clusters from being overwhelmed by highly
concurrent topic operations (see
[KIP-599](https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations)
and the quota type `controller_mutation_rate`).

**Server quotas:** Kafka also supports different types of broker-side
quotas. For example, administrators can set a limit on the rate with
which the [broker accepts new connections](../configuration#brokerconfigs_max.connection.creation.rate), set the
[maximum number of connections per broker](../configuration#brokerconfigs_max.connections), or set the maximum number of
connections allowed [from a specific IP address](../configuration#brokerconfigs_max.connections.per.ip).

For more information, please refer to the [quota overview](../design#design_quotas) and [how to set quotas](#quotas).

### Monitoring and Metering {#multitenancy-monitoring .anchor-link}

[Monitoring](#monitoring) is a broader subject that is covered
[elsewhere](#monitoring) in the documentation. Administrators of any
Kafka environment, but especially multi-tenant ones, should set up
monitoring according to these instructions. Kafka supports a wide range
of metrics, such as the rate of failed authentication attempts, request
latency, consumer lag, total number of consumer groups, metrics on the
quotas described in the previous section, and many more.

For example, monitoring can be configured to track the size of
topic-partitions (with the JMX metric
`kafka.log.Log.Size.<TOPIC-NAME>`), and thus the total size of data
stored in a topic. You can then define alerts when tenants on shared
clusters are getting close to using too much storage space.

### Multi-Tenancy and Geo-Replication {#multitenancy-georeplication}

Kafka lets you share data across different clusters, which may be
located in different geographical regions, data centers, and so on.
Apart from use cases such as disaster recovery, this functionality is
useful when a multi-tenant setup requires inter-cluster data sharing.
See the section [Geo-Replication (Cross-Cluster Data Mirroring)](#georeplication) 
for more information.

### Further considerations {#multitenancy-more .anchor-link}

**Data contracts:** You may need to define data contracts between the
producers and the consumers of data in a cluster, using event schemas.
This ensures that events written to Kafka can always be read properly
again, and prevents malformed or corrupt events being written. The best
way to achieve this is to deploy a so-called schema registry alongside
the cluster. (Kafka does not include a schema registry, but there are
third-party implementations available.) A schema registry manages the
event schemas and maps the schemas to topics, so that producers know
which topics are accepting which types (schemas) of events, and
consumers know how to read and parse events in a topic. Some registry
implementations provide further functionality, such as schema evolution,
storing a history of all schemas, and schema compatibility settings.

## 6.5 Kafka Configuration {#config .anchor-link}

### Important Client Configurations {#clientconfig .anchor-link}

The most important producer configurations are:

-   acks
-   compression
-   batch size

The most important consumer configuration is the fetch size.

All configurations are documented in the [configuration](../configuration)
section.

### A Production Server Config {#prodconfig .anchor-link}

Here is an example production server configuration:

```java-properties
# ZooKeeper
zookeeper.connect=[list of ZooKeeper servers]

# Log configuration
num.partitions=8
default.replication.factor=3
log.dir=[List of directories. Kafka should have its own dedicated disk(s) or SSD(s).]

# Other configurations
broker.id=[An integer. Start with 0 and increment by 1 for each new broker.]
listeners=[list of listeners]
auto.create.topics.enable=false
min.insync.replicas=2
queued.max.requests=[number of concurrent requests]
```

Our client configuration varies a fair amount between different use
cases.

## 6.6 Java Version {#java .anchor-link}

Java 8, Java 11, and Java 17 are supported. Note that Java 8 support has
been deprecated since Apache Kafka 3.0 and will be removed in Apache
Kafka 4.0. Java 11 and later versions perform significantly better if
TLS is enabled, so they are highly recommended (they also include a
number of other performance improvements: G1GC, CRC32C, Compact Strings,
Thread-Local Handshakes and more). From a security perspective, we
recommend the latest released patch version as older freely available
versions have disclosed security vulnerabilities. Typical arguments for
running Kafka with OpenJDK-based Java implementations (including Oracle
JDK) are:

```
  -Xmx6g -Xms6g -XX:MetaspaceSize=96m -XX:+UseG1GC
  -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M
  -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent
```

For reference, here are the stats for one of LinkedIn\'s busiest
clusters (at peak) that uses said Java arguments:

-   60 brokers
-   50k partitions (replication factor 2)
-   800k messages/sec in
-   300 MB/sec inbound, 1 GB/sec+ outbound

All of the brokers in that cluster have a 90% GC pause time of about
21ms with less than 1 young GC per second.

## 6.7 Hardware and OS {#hwandos .anchor-link}

We are using dual quad-core Intel Xeon machines with 24GB of memory.

You need sufficient memory to buffer active readers and writers. You can
do a back-of-the-envelope estimate of memory needs by assuming you want
to be able to buffer for 30 seconds and compute your memory need as
write_throughput\*30.

The disk throughput is important. We have 8x7200 rpm SATA drives. In
general disk throughput is the performance bottleneck, and more disks is
better. Depending on how you configure flush behavior you may or may not
benefit from more expensive disks (if you force flush often then higher
RPM SAS drives may be better).

### OS {#os .anchor-link}

Kafka should run well on any unix system and has been tested on Linux
and Solaris.

We have seen a few issues running on Windows and Windows is not
currently a well-supported platform though we would be happy to change
that.

It is unlikely to require much OS-level tuning, but there are three
potentially important OS-level configurations:

-   File descriptor limits: Kafka uses file descriptors for log segments
    and open connections. If a broker hosts many partitions, consider
    that the broker needs at least
    (number_of_partitions)\*(partition_size/segment_size) to track all
    log segments in addition to the number of connections the broker
    makes. We recommend at least 100000 allowed file descriptors for the
    broker processes as a starting point. Note: The mmap() function adds
    an extra reference to the file associated with the file descriptor
    fildes which is not removed by a subsequent close() on that file
    descriptor. This reference is removed when there are no more
    mappings to the file.
-   Max socket buffer size: can be increased to enable high-performance
    data transfer between data centers as [described here](http://www.psc.edu/index.php/networking/641-tcp-tune).
-   Maximum number of memory map areas a process may have (aka
    vm.max_map_count). [See the Linux kernel documentation](http://kernel.org/doc/Documentation/sysctl/vm.txt).
    You should keep an eye at this OS-level property when considering
    the maximum number of partitions a broker may have. By default, on a
    number of Linux systems, the value of vm.max_map_count is somewhere
    around 65535. Each log segment, allocated per partition, requires a
    pair of index/timeindex files, and each of these files consumes 1
    map area. In other words, each log segment uses 2 map areas. Thus,
    each partition requires minimum 2 map areas, as long as it hosts a
    single log segment. That is to say, creating 50000 partitions on a
    broker will result allocation of 100000 map areas and likely cause
    broker crash with OutOfMemoryError (Map failed) on a system with
    default vm.max_map_count. Keep in mind that the number of log
    segments per partition varies depending on the segment size, load
    intensity, retention policy and, generally, tends to be more than
    one.

### Disks and Filesystem {#diskandfs .anchor-link}

We recommend using multiple drives to get good throughput and not
sharing the same drives used for Kafka data with application logs or
other OS filesystem activity to ensure good latency. You can either RAID
these drives together into a single volume or format and mount each
drive as its own directory. Since Kafka has replication the redundancy
provided by RAID can also be provided at the application level. This
choice has several tradeoffs.

If you configure multiple data directories partitions will be assigned
round-robin to data directories. Each partition will be entirely in one
of the data directories. If data is not well balanced among partitions
this can lead to load imbalance between disks.

RAID can potentially do better at balancing load between disks (although
it doesn\'t always seem to) because it balances load at a lower level.
The primary downside of RAID is that it is usually a big performance hit
for write throughput and reduces the available disk space.

Another potential benefit of RAID is the ability to tolerate disk
failures. However our experience has been that rebuilding the RAID array
is so I/O intensive that it effectively disables the server, so this
does not provide much real availability improvement.

### Application vs. OS Flush Management {#appvsosflush .anchor-link}

Kafka always immediately writes all data to the filesystem and supports
the ability to configure the flush policy that controls when data is
forced out of the OS cache and onto disk using the flush. This flush
policy can be controlled to force data to disk after a period of time or
after a certain number of messages has been written. There are several
choices in this configuration.

Kafka must eventually call fsync to know that data was flushed. When
recovering from a crash for any log segment not known to be fsync\'d
Kafka will check the integrity of each message by checking its CRC and
also rebuild the accompanying offset index file as part of the recovery
process executed on startup.

Note that durability in Kafka does not require syncing data to disk, as
a failed node will always recover from its replicas.

We recommend using the default flush settings which disable application
fsync entirely. This means relying on the background flush done by the
OS and Kafka\'s own background flush. This provides the best of all
worlds for most uses: no knobs to tune, great throughput and latency,
and full recovery guarantees. We generally feel that the guarantees
provided by replication are stronger than sync to local disk, however
the paranoid still may prefer having both and application level fsync
policies are still supported.

The drawback of using application level flush settings is that it is
less efficient in its disk usage pattern (it gives the OS less leeway to
re-order writes) and it can introduce latency as fsync in most Linux
filesystems blocks writes to the file whereas the background flushing
does much more granular page-level locking.

In general you don\'t need to do any low-level tuning of the filesystem,
but in the next few sections we will go over some of this in case it is
useful.

### Understanding Linux OS Flush Behavior {#linuxflush .anchor-link}

In Linux, data written to the filesystem is maintained in
[pagecache](http://en.wikipedia.org/wiki/Page_cache) until it must be
written out to disk (due to an application-level fsync or the OS\'s own
flush policy). The flushing of data is done by a set of background
threads called pdflush (or in post 2.6.32 kernels \"flusher threads\").

Pdflush has a configurable policy that controls how much dirty data can
be maintained in cache and for how long before it must be written back
to disk. This policy is described
[here](http://web.archive.org/web/20160518040713/http://www.westnet.com/~gsmith/content/linux-pdflush.htm).
When Pdflush cannot keep up with the rate of data being written it will
eventually cause the writing process to block incurring latency in the
writes to slow down the accumulation of data.

You can see the current state of OS memory usage by doing

```shell {linenos=false}
 > cat /proc/meminfo 
```

The meaning of these values are described in the link above.

Using pagecache has several advantages over an in-process cache for
storing data that will be written out to disk:

-   The I/O scheduler will batch together consecutive small writes into
    bigger physical writes which improves throughput.
-   The I/O scheduler will attempt to re-sequence writes to minimize
    movement of the disk head which improves throughput.
-   It automatically uses all the free memory on the machine

### Filesystem Selection {#filesystems .anchor-link}

Kafka uses regular files on disk, and as such it has no hard dependency
on a specific filesystem. The two filesystems which have the most usage,
however, are EXT4 and XFS. Historically, EXT4 has had more usage, but
recent improvements to the XFS filesystem have shown it to have better
performance characteristics for Kafka\'s workload with no compromise in
stability.

Comparison testing was performed on a cluster with significant message
loads, using a variety of filesystem creation and mount options. The
primary metric in Kafka that was monitored was the \"Request Local
Time\", indicating the amount of time append operations were taking. XFS
resulted in much better local times (160ms vs. 250ms+ for the best EXT4
configuration), as well as lower average wait times. The XFS performance
also showed less variability in disk performance.

#### General Filesystem Notes {#generalfs .anchor-link}

For any filesystem used for data directories, on Linux systems, the
following options are recommended to be used at mount time:

-   noatime: This option disables updating of a file\'s atime (last
    access time) attribute when the file is read. This can eliminate a
    significant number of filesystem writes, especially in the case of
    bootstrapping consumers. Kafka does not rely on the atime attributes
    at all, so it is safe to disable this.

#### XFS Notes {#xfs .anchor-link}

The XFS filesystem has a significant amount of auto-tuning in place, so
it does not require any change in the default settings, either at
filesystem creation time or at mount. The only tuning parameters worth
considering are:

-   largeio: This affects the preferred I/O size reported by the stat
    call. While this can allow for higher performance on larger disk
    writes, in practice it had minimal or no effect on performance.
-   nobarrier: For underlying devices that have battery-backed cache,
    this option can provide a little more performance by disabling
    periodic write flushes. However, if the underlying device is
    well-behaved, it will report to the filesystem that it does not
    require flushes, and this option will have no effect.

#### EXT4 Notes {#ext4 .anchor-link}

EXT4 is a serviceable choice of filesystem for the Kafka data
directories, however getting the most performance out of it will require
adjusting several mount options. In addition, these options are
generally unsafe in a failure scenario, and will result in much more
data loss and corruption. For a single broker failure, this is not much
of a concern as the disk can be wiped and the replicas rebuilt from the
cluster. In a multiple-failure scenario, such as a power outage, this
can mean underlying filesystem (and therefore data) corruption that is
not easily recoverable. The following options can be adjusted:

-   data=writeback: Ext4 defaults to data=ordered which puts a strong
    order on some writes. Kafka does not require this ordering as it
    does very paranoid data recovery on all unflushed log. This setting
    removes the ordering constraint and seems to significantly reduce
    latency.
-   Disabling journaling: Journaling is a tradeoff: it makes reboots
    faster after server crashes but it introduces a great deal of
    additional locking which adds variance to write performance. Those
    who don\'t care about reboot time and want to reduce a major source
    of write latency spikes can turn off journaling entirely.
-   commit=num_secs: This tunes the frequency with which ext4 commits to
    its metadata journal. Setting this to a lower value reduces the loss
    of unflushed data during a crash. Setting this to a higher value
    will improve throughput.
-   nobh: This setting controls additional ordering guarantees when
    using data=writeback mode. This should be safe with Kafka as we do
    not depend on write ordering and improves throughput and latency.
-   delalloc: Delayed allocation means that the filesystem avoid
    allocating any blocks until the physical write occurs. This allows
    ext4 to allocate a large extent instead of smaller pages and helps
    ensure the data is written sequentially. This feature is great for
    throughput. It does seem to involve some locking in the filesystem
    which adds a bit of latency variance.

### Replace KRaft Controller Disk {#replace_disk .anchor-link}

When Kafka is configured to use KRaft, the controllers store the cluster
metadata in the directory specified in `metadata.log.dir` \-- or the
first log directory, if `metadata.log.dir` is not configured. See the
documentation for `metadata.log.dir` for details.

If the data in the cluster metdata directory is lost either because of
hardware failure or the hardware needs to be replaced, care should be
taken when provisioning the new controller node. The new controller node
should not be formatted and started until the majority of the
controllers have all of the committed data. To determine if the majority
of the controllers have the committed data, run the
`kafka-metadata-quorum.sh` tool to describe the replication status:

```shell {linenos=false}
 > bin/kafka-metadata-quorum.sh --bootstrap-server broker_host:port describe --replication
 NodeId  LogEndOffset    Lag     LastFetchTimestamp      LastCaughtUpTimestamp   Status
 1       25806           0       1662500992757           1662500992757           Leader
 ...     ...             ...     ...                     ...                     ...
  
```

Check and wait until the `Lag` is small for a majority of the
controllers. If the leader\'s end offset is not increasing, you can wait
until the lag is 0 for a majority; otherwise, you can pick the latest
leader end offset and wait until all replicas have reached it. Check and
wait until the `LastFetchTimestamp` and `LastCaughtUpTimestamp` are
close to each other for the majority of the controllers. At this point
it is safer to format the controller\'s metadata log directory. This can
be done by running the `kafka-storage.sh` command.

```shell {linenos=false}
 > bin/kafka-storage.sh format --cluster-id uuid --config server_properties
```

It is possible for the `bin/kafka-storage.sh format` command above to
fail with a message like `Log directory ... is already formatted`. This
can happend when combined mode is used and only the metadata log
directory was lost but not the others. In that case and only in that
case, can you run the `kafka-storage.sh format` command with the
`--ignore-formatted` option.

Start the KRaft controller after formatting the log directories.

```shell {linenos=false}
 > /bin/kafka-server-start.sh server_properties
```

## 6.8 Monitoring {#monitoring .anchor-link}

Kafka uses Yammer Metrics for metrics reporting in the server. The Java
clients use Kafka Metrics, a built-in metrics registry that minimizes
transitive dependencies pulled into client applications. Both expose
metrics via JMX and can be configured to report stats using pluggable
stats reporters to hook up to your monitoring system.

All Kafka rate metrics have a corresponding cumulative count metric with
suffix `-total`. For example, `records-consumed-rate` has a
corresponding metric named `records-consumed-total`.

The easiest way to see the available metrics is to fire up jconsole and
point it at a running kafka client or server; this will allow browsing
all metrics with JMX.

### Security Considerations for Remote Monitoring using JMX {#remote_jmx .anchor-link}

Apache Kafka disables remote JMX by default. You can enable remote
monitoring using JMX by setting the environment variable `JMX_PORT` for
processes started using the CLI or standard Java system properties to
enable remote JMX programmatically. You must enable security when
enabling remote JMX in production scenarios to ensure that unauthorized
users cannot monitor or control your broker or application as well as
the platform on which these are running. Note that authentication is
disabled for JMX by default in Kafka and security configs must be
overridden for production deployments by setting the environment
variable `KAFKA_JMX_OPTS` for processes started using the CLI or by
setting appropriate Java system properties. See 
[Monitoring and Management Using JMX Technology](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html)
for details on securing JMX.

We do graphs and alerting on the following metrics:

{{< ops-metrics-table kafka-metrics >}}

### KRaft Monitoring Metrics {#kraft_monitoring .anchor-link}

The set of metrics that allow monitoring of the KRaft quorum and the
metadata log.\
Note that some exposed metrics depend on the role of the node as defined
by `process.roles`

#### KRaft Quorum Monitoring Metrics {#kraft_quorum_monitoring .anchor-link}

These metrics are reported on both Controllers and Brokers in a KRaft
Cluster

{{< ops-metrics-table kraft-quorum-metrics >}}

#### KRaft Controller Monitoring Metrics {#kraft_controller_monitoring .anchor-link}

{{< ops-metrics-table kraft-controller-metrics >}}

#### KRaft Broker Monitoring Metrics {#kraft_broker_monitoring .anchor-link}

{{< ops-metrics-table kraft-broker-metrics >}}

### Common monitoring metrics for producer/consumer/connect/streams {#selector_monitoring}

The following metrics are available on
producer/consumer/connector/streams instances. For specific metrics,
please see following sections.

{{< ops-metrics-table client-common-metrics >}}

### Common Per-broker metrics for producer/consumer/connect/streams {#common_node_monitoring}

The following metrics are available on
producer/consumer/connector/streams instances. For specific metrics,
please see following sections.

{{< ops-metrics-table client-common-per-broker-metrics >}}

### Producer monitoring {#producer_monitoring .anchor-link}

The following metrics are available on producer instances.

{{< ops-metrics-table producer-metrics >}}

#### Producer Sender Metrics {#producer_sender_monitoring .anchor-link}

{{< metrics-table producer_metrics >}}

### Consumer monitoring {#consumer_monitoring .anchor-link}

The following metrics are available on consumer instances.

{{< ops-metrics-table consumer-metrics >}}

#### Consumer Group Metrics {#consumer_group_monitoring .anchor-link}

{{< ops-metrics-table consumer-group-metrics >}}

#### Consumer Fetch Metrics {#consumer_fetch_monitoring .anchor-link}

{{< metrics-table consumer_metrics >}}

### Connect Monitoring {#connect_monitoring .anchor-link}

A Connect worker process contains all the producer and consumer metrics
as well as metrics specific to Connect. The worker process itself has a
number of metrics, while each connector and task have additional
metrics.

{{< metrics-table connect_metrics >}}

### Streams Monitoring {#kafka_streams_monitoring .anchor-link}

A Kafka Streams instance contains all the producer and consumer metrics
as well as additional metrics specific to Streams. The metrics have
three recording levels: `info`, `debug`, and `trace`.

Note that the metrics have a 4-layer hierarchy. At the top level there
are client-level metrics for each started Kafka Streams client. Each
client has stream threads, with their own metrics. Each stream thread
has tasks, with their own metrics. Each task has a number of processor
nodes, with their own metrics. Each task also has a number of state
stores and record caches, all with their own metrics.

Use the following configuration option to specify which metrics you want
collected:

    metrics.recording.level="info"

#### Client Metrics {#kafka_streams_client_monitoring .anchor-link}

All of the following metrics have a recording level of `info`:

{{< ops-metrics-table streams-client-metrics >}}

#### Thread Metrics {#kafka_streams_thread_monitoring .anchor-link}

All of the following metrics have a recording level of `info`:

{{< ops-metrics-table streams-thread-metrics >}}

#### Task Metrics {#kafka_streams_task_monitoring .anchor-link}

All of the following metrics have a recording level of `debug`, except
for the dropped-records-\* and active-process-ratio metrics which have a
recording level of `info`:

{{< ops-metrics-table streams-task-metrics >}}

#### Processor Node Metrics {#kafka_streams_node_monitoring .anchor-link}

The following metrics are only available on certain types of nodes,
i.e., the process-\* metrics are only available for source processor
nodes, the suppression-emit-\* metrics are only available for
suppression operation nodes, and the record-e2e-latency-\* metrics are
only available for source processor nodes and terminal nodes (nodes
without successor nodes). All of the metrics have a recording level of
`debug`, except for the record-e2e-latency-\* metrics which have a
recording level of `info`:

{{< ops-metrics-table streams-node-metrics >}}

#### State Store Metrics {#kafka_streams_store_monitoring .anchor-link}

All of the following metrics have a recording level of `debug`, except
for the record-e2e-latency-\* metrics which have a recording level
`trace`. Note that the `store-scope` value is specified in
`StoreSupplier#metricsScope()` for user\'s customized state stores; for
built-in state stores, currently we have:

-   `in-memory-state`
-   `in-memory-lru-state`
-   `in-memory-window-state`
-   `in-memory-suppression` (for suppression buffers)
-   `rocksdb-state` (for RocksDB backed key-value store)
-   `rocksdb-window-state` (for RocksDB backed window store)
-   `rocksdb-session-state` (for RocksDB backed session store)

Metrics suppression-buffer-size-avg, suppression-buffer-size-max,
suppression-buffer-count-avg, and suppression-buffer-count-max are only
available for suppression buffers. All other metrics are not available
for suppression buffers.

{{< ops-metrics-table streams-store-metrics >}}

#### RocksDB Metrics {#kafka_streams_rocksdb_monitoring .anchor-link}

RocksDB metrics are grouped into statistics-based metrics and
properties-based metrics. The former are recorded from statistics that a
RocksDB state store collects whereas the latter are recorded from
properties that RocksDB exposes. Statistics collected by RocksDB provide
cumulative measurements over time, e.g. bytes written to the state
store. Properties exposed by RocksDB provide current measurements, e.g.,
the amount of memory currently used. Note that the `store-scope` for
built-in RocksDB state stores are currently the following:

-   `rocksdb-state` (for RocksDB backed key-value store)
-   `rocksdb-window-state` (for RocksDB backed window store)
-   `rocksdb-session-state` (for RocksDB backed session store)

**RocksDB Statistics-based Metrics:** All of the following
statistics-based metrics have a recording level of `debug` because
collecting statistics in 
[RocksDB may have an impact on performance](https://github.com/facebook/rocksdb/wiki/Statistics#stats-level-and-performance-costs).
Statistics-based metrics are collected every minute from the RocksDB
state stores. If a state store consists of multiple RocksDB instances,
as is the case for WindowStores and SessionStores, each metric reports
an aggregation over the RocksDB instances of the state store.

{{< ops-metrics-table streams-rocksdb-stats-metrics >}}

**RocksDB Properties-based Metrics:** All of the following
properties-based metrics have a recording level of `info` and are
recorded when the metrics are accessed. If a state store consists of
multiple RocksDB instances, as is the case for WindowStores and
SessionStores, each metric reports the sum over all the RocksDB
instances of the state store, except for the block cache metrics
`block-cache-*`. The block cache metrics report the sum over all RocksDB
instances if each instance uses its own block cache, and they report the
recorded value from only one instance if a single block cache is shared
among all instances.

{{< ops-metrics-table streams-rocksdb-props-metrics >}}

#### Record Cache Metrics {#kafka_streams_cache_monitoring .anchor-link}

All of the following metrics have a recording level of `debug`:

{{< ops-metrics-table streams-cache-metrics >}}

### Others {#others_monitoring .anchor-link}

We recommend monitoring GC time and other stats and various server stats
such as CPU utilization, I/O service time, etc. On the client side, we
recommend monitoring the message/byte rate (global and per topic),
request rate/size/time, and on the consumer side, max lag in messages
among all partitions and min fetch request rate. For a consumer to keep
up, max lag needs to be less than a threshold and min fetch rate needs
to be larger than 0.

## 6.9 ZooKeeper {#zk .anchor-link}

### Stable version {#zkversion .anchor-link}

The current stable branch is 3.5. Kafka is regularly updated to include
the latest release in the 3.5 series.

### Operationalizing ZooKeeper {#zkops .anchor-link}

Operationally, we do the following for a healthy ZooKeeper installation:

-   Redundancy in the physical/hardware/network layout: try not to put
    them all in the same rack, decent (but don\'t go nuts) hardware, try
    to keep redundant power and network paths, etc. A typical ZooKeeper
    ensemble has 5 or 7 servers, which tolerates 2 and 3 servers down,
    respectively. If you have a small deployment, then using 3 servers
    is acceptable, but keep in mind that you\'ll only be able to
    tolerate 1 server down in this case.
-   I/O segregation: if you do a lot of write type traffic you\'ll
    almost definitely want the transaction logs on a dedicated disk
    group. Writes to the transaction log are synchronous (but batched
    for performance), and consequently, concurrent writes can
    significantly affect performance. ZooKeeper snapshots can be one
    such a source of concurrent writes, and ideally should be written on
    a disk group separate from the transaction log. Snapshots are
    written to disk asynchronously, so it is typically ok to share with
    the operating system and message log files. You can configure a
    server to use a separate disk group with the dataLogDir parameter.
-   Application segregation: Unless you really understand the
    application patterns of other apps that you want to install on the
    same box, it can be a good idea to run ZooKeeper in isolation
    (though this can be a balancing act with the capabilities of the
    hardware).
-   Use care with virtualization: It can work, depending on your cluster
    layout and read/write patterns and SLAs, but the tiny overheads
    introduced by the virtualization layer can add up and throw off
    ZooKeeper, as it can be very time sensitive
-   ZooKeeper configuration: It\'s java, make sure you give it
    \'enough\' heap space (We usually run them with 3-5G, but that\'s
    mostly due to the data set size we have here). Unfortunately we
    don\'t have a good formula for it, but keep in mind that allowing
    for more ZooKeeper state means that snapshots can become large, and
    large snapshots affect recovery time. In fact, if the snapshot
    becomes too large (a few gigabytes), then you may need to increase
    the initLimit parameter to give enough time for servers to recover
    and join the ensemble.
-   Monitoring: Both JMX and the 4 letter words (4lw) commands are very
    useful, they do overlap in some cases (and in those cases we prefer
    the 4 letter commands, they seem more predictable, or at the very
    least, they work better with the LI monitoring infrastructure)
-   Don\'t overbuild the cluster: large clusters, especially in a write
    heavy usage pattern, means a lot of intracluster communication
    (quorums on the writes and subsequent cluster member updates), but
    don\'t underbuild it (and risk swamping the cluster). Having more
    servers adds to your read capacity.

Overall, we try to keep the ZooKeeper system as small as will handle the
load (plus standard growth capacity planning) and as simple as possible.
We try not to do anything fancy with the configuration or application
layout as compared to the official release as well as keep it as self
contained as possible. For these reasons, we tend to skip the OS
packaged versions, since it has a tendency to try to put things in the
OS standard hierarchy, which can be \'messy\', for want of a better way
to word it.

## 6.10 KRaft {#kraft .anchor-link}

### Configuration {#kraft_config .anchor-link}

#### Process Roles {#kraft_role .anchor-link}

In KRaft mode each Kafka server can be configured as a controller, a
broker, or both using the `process.roles` property. This property can
have the following values:

-   If `process.roles` is set to `broker`, the server acts as a broker.
-   If `process.roles` is set to `controller`, the server acts as a
    controller.
-   If `process.roles` is set to `broker,controller`, the server acts as
    both a broker and a controller.
-   If `process.roles` is not set at all, it is assumed to be in
    ZooKeeper mode.

Kafka servers that act as both brokers and controllers are referred to
as \"combined\" servers. Combined servers are simpler to operate for
small use cases like a development environment. The key disadvantage is
that the controller will be less isolated from the rest of the system.
For example, it is not possible to roll or scale the controllers
separately from the brokers in combined mode. Combined mode is not
recommended in critical deployment environments.

#### Controllers {#kraft_voter .anchor-link}

In KRaft mode, specific Kafka servers are selected to be controllers
(unlike the ZooKeeper-based mode, where any server can become the
Controller). The servers selected to be controllers will participate in
the metadata quorum. Each controller is either an active or a hot
standby for the current active controller.

A Kafka admin will typically select 3 or 5 servers for this role,
depending on factors like cost and the number of concurrent failures
your system should withstand without availability impact. A majority of
the controllers must be alive in order to maintain availability. With 3
controllers, the cluster can tolerate 1 controller failure; with 5
controllers, the cluster can tolerate 2 controller failures.

All of the servers in a Kafka cluster discover the quorum voters using
the `controller.quorum.voters` property. This identifies the quorum
controller servers that should be used. All the controllers must be
enumerated. Each controller is identified with their `id`, `host` and
`port` information. For example:

```java-properties
controller.quorum.voters=id1@host1:port1,id2@host2:port2,id3@host3:port3
```

If a Kafka cluster has 3 controllers named controller1, controller2 and
controller3, then controller1 may have the following configuration:

```java-properties
process.roles=controller
node.id=1
listeners=CONTROLLER://controller1.example.com:9093
controller.quorum.voters=1@controller1.example.com:9093,2@controller2.example.com:9093,3@controller3.example.com:9093
```

Every broker and controller must set the `controller.quorum.voters`
property. The node ID supplied in the `controller.quorum.voters`
property must match the corresponding id on the controller servers. For
example, on controller1, node.id must be set to 1, and so forth. Each
node ID must be unique across all the servers in a particular cluster.
No two servers can have the same node ID regardless of their
`process.roles` values.

### Storage Tool {#kraft_storage .anchor-link}

The `kafka-storage.sh random-uuid` command can be used to generate a
cluster ID for your new cluster. This cluster ID must be used when
formatting each server in the cluster with the `kafka-storage.sh format`
command.

This is different from how Kafka has operated in the past. Previously,
Kafka would format blank storage directories automatically, and also
generate a new cluster ID automatically. One reason for the change is
that auto-formatting can sometimes obscure an error condition. This is
particularly important for the metadata log maintained by the controller
and broker servers. If a majority of the controllers were able to start
with an empty log directory, a leader might be able to be elected with
missing committed data.

### Debugging {#kraft_debug .anchor-link}

#### Metadata Quorum Tool {#kraft_metadata_tool .anchor-link}

The `kafka-metadata-quorum` tool can be used to describe the runtime
state of the cluster metadata partition. For example, the following
command displays a summary of the metadata quorum:

```shell {linenos=false}
> bin/kafka-metadata-quorum.sh --bootstrap-server  broker_host:port describe --status
ClusterId:              fMCL8kv1SWm87L_Md-I2hg
LeaderId:               3002
LeaderEpoch:            2
HighWatermark:          10
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   -1
CurrentVoters:          [3000,3001,3002]
CurrentObservers:       [0,1,2]
```

#### Dump Log Tool {#kraft_dump_log .anchor-link}

The `kafka-dump-log` tool can be used to debug the log segments and
snapshots for the cluster metadata directory. The tool will scan the
provided files and decode the metadata records. For example, this
command decodes and prints the records in the first log segment:

```shell {linenos=false}
> bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000000.log
```

This command decodes and prints the recrods in the a cluster metadata
snapshot:

```shell {linenos=false}
> bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000100-0000000001.checkpoint
```

#### Metadata Shell {#kraft_shell_tool .anchor-link}

The `kafka-metadata-shell` tool can be used to interactively inspect the
state of the cluster metadata partition:

```shell {linenos=false}
> bin/kafka-metadata-shell.sh  --snapshot metadata_log_dir/__cluster_metadata-0/00000000000000000000.log
> ls /
brokers  local  metadataQuorum  topicIds  topics
> ls /topics
foo
> cat /topics/foo/0/data
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
```

### Deploying Considerations {#kraft_deployment .anchor-link}

Kafka server\'s `process.role` should be set to either `broker` or
`controller` but not both. Combined mode can be used in development
environment but it should be avoided in critical deployment environments.

For redundancy, a Kafka cluster should use 3 controllers. More than 3
servers is not recommended in critical environments. In the rare case of
a partial network failure it is possible for the cluster metadata quorum
to become unavailable. This limitation will be addresses in a future
release of Kafka.

The Kafka controllers store all of the metadata for the cluster in
memory and on disk. We believe that for a typical Kafka cluster 5GB of
main memory and 5GB of disk space on the metadata log director is
sufficient.

### Missing Features {#kraft_missing .anchor-link}

The following features are not fullying implemented in KRaft mode:

-   Configuring SCRAM users via the administrative API
-   Supporting JBOD configurations with multiple storage directories
-   Modifying certain dynamic configurations on the standalone KRaft controller
-   Delegation tokens
-   Upgrade from ZooKeeper mode
