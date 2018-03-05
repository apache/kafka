Operations
==========

.. contents::
    :local:

Here is some information on actually running Kafka as a production
system based on usage and experience at LinkedIn. Please send us any
additional tips you know of.

`6.1 Basic Kafka Operations <#basic_ops>`__
-------------------------------------------

This section will review the most common operations you will perform on
your Kafka cluster. All of the tools reviewed in this section are
available under the ``bin/`` directory of the Kafka distribution and
each tool will print details on all possible commandline options if it
is run with no arguments.

`Adding and removing topics <#basic_ops_add_topic>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You have the option of either adding topics manually or having them be
created automatically when data is first published to a non-existent
topic. If topics are auto-created then you may want to tune the default
`topic configurations <#topicconfigs>`__ used for auto-created topics.

Topics are added and modified using the topic tool:

.. code:: bash

      > bin/kafka-topics.sh --zookeeper zk_host:port/chroot --create --topic my_topic_name
            --partitions 20 --replication-factor 3 --config x=y
      

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
is discussed in greater detail in the `concepts
section <#intro_consumers>`__.

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
`here <#topicconfigs>`__.

`Modifying topics <#basic_ops_modify_topic>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can change the configuration or partitioning of a topic using the
same topic tool.

To add partitions you can do

.. code:: bash

      > bin/kafka-topics.sh --zookeeper zk_host:port/chroot --alter --topic my_topic_name
            --partitions 40
      

Be aware that one use case for partitions is to semantically partition
data, and adding partitions doesn't change the partitioning of existing
data so this may disturb consumers if they rely on that partition. That
is if data is partitioned by ``hash(key) % number_of_partitions`` then
this partitioning will potentially be shuffled by adding partitions but
Kafka will not attempt to automatically redistribute data in any way.

To add configs:

.. code:: bash

      > bin/kafka-configs.sh --zookeeper zk_host:port/chroot --entity-type topics --entity-name my_topic_name --alter --add-config x=y
      

To remove a config:

.. code:: bash

      > bin/kafka-configs.sh --zookeeper zk_host:port/chroot --entity-type topics --entity-name my_topic_name --alter --delete-config x
      

And finally deleting a topic:

.. code:: bash

      > bin/kafka-topics.sh --zookeeper zk_host:port/chroot --delete --topic my_topic_name
      

Kafka does not currently support reducing the number of partitions for a
topic.

Instructions for changing the replication factor of a topic can be found
`here <#basic_ops_increase_replication_factor>`__.

`Graceful shutdown <#basic_ops_restarting>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Kafka cluster will automatically detect any broker shutdown or
failure and elect new leaders for the partitions on that machine. This
will occur whether a server fails or it is brought down intentionally
for maintenance or configuration changes. For the latter cases Kafka
supports a more graceful mechanism for stopping a server than just
killing it. When a server is stopped gracefully it has two optimizations
it will take advantage of:

#. It will sync all its logs to disk to avoid needing to do any log
   recovery when it restarts (i.e. validating the checksum for all
   messages in the tail of the log). Log recovery takes time so this
   speeds up intentional restarts.
#. It will migrate any partitions the server is the leader for to other
   replicas prior to shutting down. This will make the leadership
   transfer faster and minimize the time each partition is unavailable
   to a few milliseconds.

Syncing the logs will happen automatically whenever the server is
stopped other than by a hard kill, but the controlled leadership
migration requires using a special setting:

.. code:: bash

          controlled.shutdown.enable=true
      

Note that controlled shutdown will only succeed if *all* the partitions
hosted on the broker have replicas (i.e. the replication factor is
greater than 1 *and* at least one of these replicas is alive). This is
generally what you want since shutting down the last replica would make
that topic partition unavailable.

`Balancing leadership <#basic_ops_leader_balancing>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whenever a broker stops or crashes leadership for that broker's
partitions transfers to other replicas. This means that by default when
the broker is restarted it will only be a follower for all its
partitions, meaning it will not be used for client reads and writes.

To avoid this imbalance, Kafka has a notion of preferred replicas. If
the list of replicas for a partition is 1,5,9 then node 1 is preferred
as the leader to either node 5 or 9 because it is earlier in the replica
list. You can have the Kafka cluster try to restore leadership to the
restored replicas by running the command:

.. code:: bash

      > bin/kafka-preferred-replica-election.sh --zookeeper zk_host:port/chroot
      

Since running this command can be tedious you can also configure Kafka
to do this automatically by setting the following configuration:

.. code:: bash

          auto.leader.rebalance.enable=true
      

`Balancing Replicas Across Racks <#basic_ops_racks>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The rack awareness feature spreads replicas of the same partition across
different racks. This extends the guarantees Kafka provides for
broker-failure to cover rack-failure, limiting the risk of data loss
should all the brokers on a rack fail at once. The feature can also be
applied to other broker groupings such as availability zones in EC2.

You can specify that a broker belongs to a particular rack by adding a
property to the broker config:

.. code:: bash

       broker.rack=my-rack-id

When a topic is `created <#basic_ops_add_topic>`__,
`modified <#basic_ops_modify_topic>`__ or replicas are
`redistributed <#basic_ops_cluster_expansion>`__, the rack constraint
will be honoured, ensuring replicas span as many racks as they can (a
partition will span min(#racks, replication-factor) different racks).

The algorithm used to assign replicas to brokers ensures that the number
of leaders per broker will be constant, regardless of how brokers are
distributed across racks. This ensures balanced throughput.

However if racks are assigned different numbers of brokers, the
assignment of replicas will not be even. Racks with fewer brokers will
get more replicas, meaning they will use more storage and put more
resources into replication. Hence it is sensible to configure an equal
number of brokers per rack.

`Mirroring data between clusters <#basic_ops_mirror_maker>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We refer to the process of replicating data *between* Kafka clusters
"mirroring" to avoid confusion with the replication that happens amongst
the nodes in a single cluster. Kafka comes with a tool for mirroring
data between Kafka clusters. The tool consumes from a source cluster and
produces to a destination cluster. A common use case for this kind of
mirroring is to provide a replica in another datacenter. This scenario
will be discussed in more detail in the next section.

You can run many such mirroring processes to increase throughput and for
fault-tolerance (if one process dies, the others will take overs the
additional load).

Data will be read from topics in the source cluster and written to a
topic with the same name in the destination cluster. In fact the mirror
maker is little more than a Kafka consumer and producer hooked together.

The source and destination clusters are completely independent entities:
they can have different numbers of partitions and the offsets will not
be the same. For this reason the mirror cluster is not really intended
as a fault-tolerance mechanism (as the consumer position will be
different); for that we recommend using normal in-cluster replication.
The mirror maker process will, however, retain and use the message key
for partitioning so order is preserved on a per-key basis.

Here is an example showing how to mirror a single topic (named
*my-topic*) from an input cluster:

.. code:: bash

      > bin/kafka-mirror-maker.sh
            --consumer.config consumer.properties
            --producer.config producer.properties --whitelist my-topic
      

Note that we specify the list of topics with the ``--whitelist`` option.
This option allows any regular expression using `Java-style regular
expressions <http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html>`__.
So you could mirror two topics named *A* and *B* using
``--whitelist 'A|B'``. Or you could mirror *all* topics using
``--whitelist '*'``. Make sure to quote any regular expression to ensure
the shell doesn't try to expand it as a file path. For convenience we
allow the use of ',' instead of '|' to specify a list of topics.

Sometimes it is easier to say what it is that you *don't* want. Instead
of using ``--whitelist`` to say what you want to mirror you can use
``--blacklist`` to say what to exclude. This also takes a regular
expression argument. However, ``--blacklist`` is not supported when the
new consumer has been enabled (i.e. when ``bootstrap.servers`` has been
defined in the consumer configuration).

Combining mirroring with the configuration
``auto.create.topics.enable=true`` makes it possible to have a replica
cluster that will automatically create and replicate all data in a
source cluster even as new topics are added.

`Checking consumer position <#basic_ops_consumer_lag>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes it's useful to see the position of your consumers. We have a
tool that will show the position of all consumers in a consumer group as
well as how far behind the end of the log they are. To run this tool on
a consumer group named *my-group* consuming a topic named *my-topic*
would look like this:

.. code:: bash

      > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group

      Note: This will only show information about consumers that use the Java consumer API (non-ZooKeeper-based consumers).

      TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
      my-topic                       0          2               4               2          consumer-1-029af89c-873c-4751-a720-cefd41a669d6   /127.0.0.1                     consumer-1
      my-topic                       1          2               3               1          consumer-1-029af89c-873c-4751-a720-cefd41a669d6   /127.0.0.1                     consumer-1
      my-topic                       2          2               3               1          consumer-2-42c1abd4-e3b2-425d-a8bb-e1ea49b29bb2   /127.0.0.1                     consumer-2
      

This tool also works with ZooKeeper-based consumers:

.. code:: bash

      > bin/kafka-consumer-groups.sh --zookeeper localhost:2181 --describe --group my-group

      Note: This will only show information about consumers that use ZooKeeper (not those using the Java consumer API).

      TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID
      my-topic                       0          2               4               2          my-group_consumer-1
      my-topic                       1          2               3               1          my-group_consumer-1
      my-topic                       2          2               3               1          my-group_consumer-2
      

`Managing Consumer Groups <#basic_ops_consumer_group>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the ConsumerGroupCommand tool, we can list, describe, or delete
consumer groups. Note that deletion is only available when the group
metadata is stored in ZooKeeper. When using the `new consumer
API <http://kafka.apache.org/documentation.html#newconsumerapi>`__
(where the broker handles coordination of partition handling and
rebalance), the group is deleted when the last committed offset for that
group expires. For example, to list all consumer groups across all
topics:

.. code:: bash

      > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

      test-consumer-group
      

To view offsets, as mentioned earlier, we "describe" the consumer group
like this:

.. code:: bash

      > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group

      TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                    HOST            CLIENT-ID
      topic3          0          241019          395308          154289          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
      topic2          1          520678          803288          282610          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
      topic3          1          241018          398817          157799          consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2
      topic1          0          854144          855809          1665            consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1
      topic2          0          460537          803290          342753          consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1
      topic3          2          243655          398812          155157          consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4
      

There are a number of additional "describe" options that can be used to
provide more detailed information about a consumer group that uses the
new consumer API:

-  --members: This option provides the list of all active members in the
   consumer group.

   .. code:: bash

             > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members

             CONSUMER-ID                                    HOST            CLIENT-ID       #PARTITIONS
             consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1       2
             consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4       1
             consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2       3
             consumer3-ecea43e4-1f01-479f-8349-f9130b75d8ee /127.0.0.1      consumer3       0
             

-  --members --verbose: On top of the information reported by the
   "--members" options above, this option also provides the partitions
   assigned to each member.

   .. code:: bash

             > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members --verbose

             CONSUMER-ID                                    HOST            CLIENT-ID       #PARTITIONS     ASSIGNMENT
             consumer1-3fc8d6f1-581a-4472-bdf3-3515b4aee8c1 /127.0.0.1      consumer1       2               topic1(0), topic2(0)
             consumer4-117fe4d3-c6c1-4178-8ee9-eb4a3954bee0 /127.0.0.1      consumer4       1               topic3(2)
             consumer2-e76ea8c3-5d30-4299-9005-47eb41f3d3c4 /127.0.0.1      consumer2       3               topic2(1), topic3(0,1)
             consumer3-ecea43e4-1f01-479f-8349-f9130b75d8ee /127.0.0.1      consumer3       0               -
             

-  --offsets: This is the default describe option and provides the same
   output as the "--describe" option.
-  --state: This option provides useful group-level information.

   .. code:: bash

             > bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --state

             COORDINATOR (ID)          ASSIGNMENT-STRATEGY       STATE                #MEMBERS
             localhost:9092 (0)        range                     Stable               4
             

If you are using the old high-level consumer and storing the group
metadata in ZooKeeper (i.e. ``offsets.storage=zookeeper``), pass
``--zookeeper`` instead of ``bootstrap-server``:

.. code:: bash

      > bin/kafka-consumer-groups.sh --zookeeper localhost:2181 --list
      

`Expanding your cluster <#basic_ops_cluster_expansion>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Adding servers to a Kafka cluster is easy, just assign them a unique
broker id and start up Kafka on your new servers. However these new
servers will not automatically be assigned any data partitions, so
unless partitions are moved to them they won't be doing any work until
new topics are created. So usually when you add machines to your cluster
you will want to migrate some existing data to these machines.

The process of migrating data is manually initiated but fully automated.
Under the covers what happens is that Kafka will add the new server as a
follower of the partition it is migrating and allow it to fully
replicate the existing data in that partition. When the new server has
fully replicated the contents of this partition and joined the in-sync
replica one of the existing replicas will delete their partition's data.

The partition reassignment tool can be used to move partitions across
brokers. An ideal partition distribution would ensure even data load and
partition sizes across all brokers. The partition reassignment tool does
not have the capability to automatically study the data distribution in
a Kafka cluster and move partitions around to attain an even load
distribution. As such, the admin has to figure out which topics or
partitions should be moved around.

The partition reassignment tool can run in 3 mutually exclusive modes:

-  --generate: In this mode, given a list of topics and a list of
   brokers, the tool generates a candidate reassignment to move all
   partitions of the specified topics to the new brokers. This option
   merely provides a convenient way to generate a partition reassignment
   plan given a list of topics and target brokers.
-  --execute: In this mode, the tool kicks off the reassignment of
   partitions based on the user provided reassignment plan. (using the
   --reassignment-json-file option). This can either be a custom
   reassignment plan hand crafted by the admin or provided by using the
   --generate option
-  --verify: In this mode, the tool verifies the status of the
   reassignment for all partitions listed during the last --execute. The
   status can be either of successfully completed, failed or in progress

`Automatically migrating data to new machines <#basic_ops_automigrate>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. code:: bash

      > cat topics-to-move.json
      {"topics": [{"topic": "foo1"},
                  {"topic": "foo2"}],
      "version":1
      }
      

Once the json file is ready, use the partition reassignment tool to
generate a candidate assignment:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --topics-to-move-json-file topics-to-move.json --broker-list "5,6" --generate
      Current partition replica assignment

      {"version":1,
      "partitions":[{"topic":"foo1","partition":2,"replicas":[1,2]},
                    {"topic":"foo1","partition":0,"replicas":[3,4]},
                    {"topic":"foo2","partition":2,"replicas":[1,2]},
                    {"topic":"foo2","partition":0,"replicas":[3,4]},
                    {"topic":"foo1","partition":1,"replicas":[2,3]},
                    {"topic":"foo2","partition":1,"replicas":[2,3]}]
      }

      Proposed partition reassignment configuration

      {"version":1,
      "partitions":[{"topic":"foo1","partition":2,"replicas":[5,6]},
                    {"topic":"foo1","partition":0,"replicas":[5,6]},
                    {"topic":"foo2","partition":2,"replicas":[5,6]},
                    {"topic":"foo2","partition":0,"replicas":[5,6]},
                    {"topic":"foo1","partition":1,"replicas":[5,6]},
                    {"topic":"foo2","partition":1,"replicas":[5,6]}]
      }
      

The tool generates a candidate assignment that will move all partitions
from topics foo1,foo2 to brokers 5,6. Note, however, that at this point,
the partition movement has not started, it merely tells you the current
assignment and the proposed new assignment. The current assignment
should be saved in case you want to rollback to it. The new assignment
should be saved in a json file (e.g. expand-cluster-reassignment.json)
to be input to the tool with the --execute option as follows:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file expand-cluster-reassignment.json --execute
      Current partition replica assignment

      {"version":1,
      "partitions":[{"topic":"foo1","partition":2,"replicas":[1,2]},
                    {"topic":"foo1","partition":0,"replicas":[3,4]},
                    {"topic":"foo2","partition":2,"replicas":[1,2]},
                    {"topic":"foo2","partition":0,"replicas":[3,4]},
                    {"topic":"foo1","partition":1,"replicas":[2,3]},
                    {"topic":"foo2","partition":1,"replicas":[2,3]}]
      }

      Save this to use as the --reassignment-json-file option during rollback
      Successfully started reassignment of partitions
      {"version":1,
      "partitions":[{"topic":"foo1","partition":2,"replicas":[5,6]},
                    {"topic":"foo1","partition":0,"replicas":[5,6]},
                    {"topic":"foo2","partition":2,"replicas":[5,6]},
                    {"topic":"foo2","partition":0,"replicas":[5,6]},
                    {"topic":"foo1","partition":1,"replicas":[5,6]},
                    {"topic":"foo2","partition":1,"replicas":[5,6]}]
      }
      

Finally, the --verify option can be used with the tool to check the
status of the partition reassignment. Note that the same
expand-cluster-reassignment.json (used with the --execute option) should
be used with the --verify option:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file expand-cluster-reassignment.json --verify
      Status of partition reassignment:
      Reassignment of partition [foo1,0] completed successfully
      Reassignment of partition [foo1,1] is in progress
      Reassignment of partition [foo1,2] is in progress
      Reassignment of partition [foo2,0] completed successfully
      Reassignment of partition [foo2,1] completed successfully
      Reassignment of partition [foo2,2] completed successfully
      

`Custom partition assignment and migration <#basic_ops_partitionassignment>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The partition reassignment tool can also be used to selectively move
replicas of a partition to a specific set of brokers. When used in this
manner, it is assumed that the user knows the reassignment plan and does
not require the tool to generate a candidate reassignment, effectively
skipping the --generate step and moving straight to the --execute step

For instance, the following example moves partition 0 of topic foo1 to
brokers 5,6 and partition 1 of topic foo2 to brokers 2,3:

The first step is to hand craft the custom reassignment plan in a json
file:

.. code:: bash

      > cat custom-reassignment.json
      {"version":1,"partitions":[{"topic":"foo1","partition":0,"replicas":[5,6]},{"topic":"foo2","partition":1,"replicas":[2,3]}]}
      

Then, use the json file with the --execute option to start the
reassignment process:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file custom-reassignment.json --execute
      Current partition replica assignment

      {"version":1,
      "partitions":[{"topic":"foo1","partition":0,"replicas":[1,2]},
                    {"topic":"foo2","partition":1,"replicas":[3,4]}]
      }

      Save this to use as the --reassignment-json-file option during rollback
      Successfully started reassignment of partitions
      {"version":1,
      "partitions":[{"topic":"foo1","partition":0,"replicas":[5,6]},
                    {"topic":"foo2","partition":1,"replicas":[2,3]}]
      }
      

The --verify option can be used with the tool to check the status of the
partition reassignment. Note that the same
expand-cluster-reassignment.json (used with the --execute option) should
be used with the --verify option:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file custom-reassignment.json --verify
      Status of partition reassignment:
      Reassignment of partition [foo1,0] completed successfully
      Reassignment of partition [foo2,1] completed successfully
      

`Decommissioning brokers <#basic_ops_decommissioning_brokers>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The partition reassignment tool does not have the ability to
automatically generate a reassignment plan for decommissioning brokers
yet. As such, the admin has to come up with a reassignment plan to move
the replica for all partitions hosted on the broker to be
decommissioned, to the rest of the brokers. This can be relatively
tedious as the reassignment needs to ensure that all the replicas are
not moved from the decommissioned broker to only one other broker. To
make this process effortless, we plan to add tooling support for
decommissioning brokers in the future.

`Increasing replication factor <#basic_ops_increase_replication_factor>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Increasing the replication factor of an existing partition is easy. Just
specify the extra replicas in the custom reassignment json file and use
it with the --execute option to increase the replication factor of the
specified partitions.

For instance, the following example increases the replication factor of
partition 0 of topic foo from 1 to 3. Before increasing the replication
factor, the partition's only replica existed on broker 5. As part of
increasing the replication factor, we will add more replicas on brokers
6 and 7.

The first step is to hand craft the custom reassignment plan in a json
file:

.. code:: bash

      > cat increase-replication-factor.json
      {"version":1,
      "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}
      

Then, use the json file with the --execute option to start the
reassignment process:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file increase-replication-factor.json --execute
      Current partition replica assignment

      {"version":1,
      "partitions":[{"topic":"foo","partition":0,"replicas":[5]}]}

      Save this to use as the --reassignment-json-file option during rollback
      Successfully started reassignment of partitions
      {"version":1,
      "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}
      

The --verify option can be used with the tool to check the status of the
partition reassignment. Note that the same
increase-replication-factor.json (used with the --execute option) should
be used with the --verify option:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file increase-replication-factor.json --verify
      Status of partition reassignment:
      Reassignment of partition [foo,0] completed successfully
      

You can also verify the increase in replication factor with the
kafka-topics tool:

.. code:: bash

      > bin/kafka-topics.sh --zookeeper localhost:2181 --topic foo --describe
      Topic:foo PartitionCount:1    ReplicationFactor:3 Configs:
        Topic: foo  Partition: 0    Leader: 5   Replicas: 5,6,7 Isr: 5,6,7
      

`Limiting Bandwidth Usage during Data Migration <#rep-throttle>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

.. code:: bash

    $ bin/kafka-reassign-partitions.sh --zookeeper myhost:2181--execute --reassignment-json-file bigger-cluster.json —throttle 50000000

When you execute this script you will see the throttle engage:

.. code:: bash

      The throttle limit was set to 50000000 B/s
      Successfully started reassignment of partitions.

Should you wish to alter the throttle, during a rebalance, say to
increase the throughput so it completes quicker, you can do this by
re-running the execute command passing the same reassignment-json-file:

.. code:: bash

    $ bin/kafka-reassign-partitions.sh --zookeeper localhost:2181  --execute --reassignment-json-file bigger-cluster.json --throttle 700000000
      There is an existing assignment running.
      The throttle limit was set to 700000000 B/s

Once the rebalance completes the administrator can check the status of
the rebalance using the --verify option. If the rebalance has completed,
the throttle will be removed via the --verify command. It is important
that administrators remove the throttle in a timely manner once
rebalancing completes by running the command with the --verify option.
Failure to do so could cause regular replication traffic to be
throttled.

When the --verify option is executed, and the reassignment has
completed, the script will confirm that the throttle was removed:

.. code:: bash

      > bin/kafka-reassign-partitions.sh --zookeeper localhost:2181  --verify --reassignment-json-file bigger-cluster.json
      Status of partition reassignment:
      Reassignment of partition [my-topic,1] completed successfully
      Reassignment of partition [mytopic,0] completed successfully
      Throttle was removed.

The administrator can also validate the assigned configs using the
kafka-configs.sh. There are two pairs of throttle configuration used to
manage the throttling process. The throttle value itself. This is
configured, at a broker level, using the dynamic properties:

.. code:: bash

    leader.replication.throttled.rate
      follower.replication.throttled.rate

There is also an enumerated set of throttled replicas:

.. code:: bash

    leader.replication.throttled.replicas
      follower.replication.throttled.replicas

Which are configured per topic. All four config values are automatically
assigned by kafka-reassign-partitions.sh (discussed below).

To view the throttle limit configuration:

.. code:: bash

      > bin/kafka-configs.sh --describe --zookeeper localhost:2181 --entity-type brokers
      Configs for brokers '2' are leader.replication.throttled.rate=700000000,follower.replication.throttled.rate=700000000
      Configs for brokers '1' are leader.replication.throttled.rate=700000000,follower.replication.throttled.rate=700000000

This shows the throttle applied to both leader and follower side of the
replication protocol. By default both sides are assigned the same
throttled throughput value.

To view the list of throttled replicas:

.. code:: bash

      > bin/kafka-configs.sh --describe --zookeeper localhost:2181 --entity-type topics
      Configs for topic 'my-topic' are leader.replication.throttled.replicas=1:102,0:101,
          follower.replication.throttled.replicas=1:101,0:102

Here we see the leader throttle is applied to partition 1 on broker 102
and partition 0 on broker 101. Likewise the follower throttle is applied
to partition 1 on broker 101 and partition 0 on broker 102.

By default kafka-reassign-partitions.sh will apply the leader throttle
to all replicas that exist before the rebalance, any one of which might
be leader. It will apply the follower throttle to all move destinations.
So if there is a partition with replicas on brokers 101,102, being
reassigned to 102,103, a leader throttle, for that partition, would be
applied to 101,102 and a follower throttle would be applied to 103 only.

If required, you can also use the --alter switch on kafka-configs.sh to
alter the throttle configurations manually.

Safe usage of throttled replication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some care should be taken when using throttled replication. In
particular:

*(1) Throttle Removal:*

The throttle should be removed in a timely manner once reassignment
completes (by running kafka-reassign-partitions —verify).

*(2) Ensuring Progress:*

If the throttle is set too low, in comparison to the incoming write
rate, it is possible for replication to not make progress. This occurs
when:

::

    max(BytesInPerSec) > throttle

Where BytesInPerSec is the metric that monitors the write throughput of
producers into each broker.

The administrator can monitor whether replication is making progress,
during the rebalance, using the metric:

::

    kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=([-.\w]+),topic=([-.\w]+),partition=([0-9]+)

The lag should constantly decrease during replication. If the metric
does not decrease the administrator should increase the throttle
throughput as described above.

`Setting quotas <#quotas>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Quotas overrides and defaults may be configured at (user, client-id),
user or client-id levels as described `here <#design_quotas>`__. By
default, clients receive an unlimited quota. It is possible to set
custom quotas for each (user, client-id), user or client-id group.

Configure custom quota for (user=user1, client-id=clientA):

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
      Updated config for entity: user-principal 'user1', client-id 'clientA'.
      

Configure custom quota for user=user1:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1
      Updated config for entity: user-principal 'user1'.
      

Configure custom quota for client-id=clientA:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-name clientA
      Updated config for entity: client-id 'clientA'.
      

It is possible to set default quotas for each (user, client-id), user or
client-id group by specifying *--entity-default* option instead of
*--entity-name*.

Configure default client-id quota for user=userA:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-default
      Updated config for entity: user-principal 'user1', default client-id.
      

Configure default quota for user:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-default
      Updated config for entity: default user-principal.
      

Configure default quota for client-id:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-default
      Updated config for entity: default client-id.
      

Here's how to describe the quota for a given (user, client-id):

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --describe --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
      Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      

Describe quota for a given user:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --describe --entity-type users --entity-name user1
      Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      

Describe quota for a given client-id:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --describe --entity-type clients --entity-name clientA
      Configs for client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      

If entity name is not specified, all entities of the specified type are
described. For example, describe all users:

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --describe --entity-type users
      Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      Configs for default user-principal are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      

Similarly for (user, client):

.. code:: bash

      > bin/kafka-configs.sh  --zookeeper localhost:2181 --describe --entity-type users --entity-type clients
      Configs for user-principal 'user1', default client-id are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
      

It is possible to set default quotas that apply to all client-ids by
setting these configs on the brokers. These properties are applied only
if quota overrides or defaults are not configured in Zookeeper. By
default, each client-id receives an unlimited quota. The following sets
the default quota per producer and consumer client-id to 10MB/sec.

.. code:: bash

        quota.producer.default=10485760
        quota.consumer.default=10485760
      

Note that these properties are being deprecated and may be removed in a
future release. Defaults configured using kafka-configs.sh take
precedence over these properties.

`6.2 Datacenters <#datacenters>`__
----------------------------------

Some deployments will need to manage a data pipeline that spans multiple
datacenters. Our recommended approach to this is to deploy a local Kafka
cluster in each datacenter with application instances in each datacenter
interacting only with their local cluster and mirroring between clusters
(see the documentation on the `mirror maker
tool <#basic_ops_mirror_maker>`__ for how to do this).

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
``socket.send.buffer.bytes`` and ``socket.receive.buffer.bytes``
configurations. The appropriate way to set this is documented
`here <http://en.wikipedia.org/wiki/Bandwidth-delay_product>`__.

It is generally *not* advisable to run a *single* Kafka cluster that
spans multiple datacenters over a high-latency link. This will incur
very high replication latency both for Kafka writes and ZooKeeper
writes, and neither Kafka nor ZooKeeper will remain available in all
locations if the network between locations is unavailable.

`6.3 Kafka Configuration <#config>`__
-------------------------------------

`Important Client Configurations <#clientconfig>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most important old Scala producer configurations control

-  acks
-  compression
-  sync vs async production
-  batch size (for async producers)

The most important new Java producer configurations control

-  acks
-  compression
-  batch size

The most important consumer configuration is the fetch size.

All configurations are documented in the
`configuration <#configuration>`__ section.

`A Production Server Config <#prodconfig>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example production server configuration:

.. code:: bash

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
      

Our client configuration varies a fair amount between different use
cases.

`6.4 Java Version <#java>`__
----------------------------

From a security perspective, we recommend you use the latest released
version of JDK 1.8 as older freely available versions have disclosed
security vulnerabilities. LinkedIn is currently running JDK 1.8 u5
(looking to upgrade to a newer version) with the G1 collector. If you
decide to use the G1 collector (the current default) and you are still
on JDK 1.7, make sure you are on u51 or newer. LinkedIn tried out u21 in
testing, but they had a number of problems with the GC implementation in
that version. LinkedIn's tuning looks like this:

.. code:: bash

      -Xmx6g -Xms6g -XX:MetaspaceSize=96m -XX:+UseG1GC
      -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M
      -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80
      

For reference, here are the stats on one of LinkedIn's busiest clusters
(at peak):

-  60 brokers
-  50k partitions (replication factor 2)
-  800k messages/sec in
-  300 MB/sec inbound, 1 GB/sec+ outbound

The tuning looks fairly aggressive, but all of the brokers in that
cluster have a 90% GC pause time of about 21ms, and they're doing less
than 1 young GC per second.

`6.5 Hardware and OS <#hwandos>`__
----------------------------------

We are using dual quad-core Intel Xeon machines with 24GB of memory.

You need sufficient memory to buffer active readers and writers. You can
do a back-of-the-envelope estimate of memory needs by assuming you want
to be able to buffer for 30 seconds and compute your memory need as
write_throughput*30.

The disk throughput is important. We have 8x7200 rpm SATA drives. In
general disk throughput is the performance bottleneck, and more disks is
better. Depending on how you configure flush behavior you may or may not
benefit from more expensive disks (if you force flush often then higher
RPM SAS drives may be better).

`OS <#os>`__
~~~~~~~~~~~~

Kafka should run well on any unix system and has been tested on Linux
and Solaris.

We have seen a few issues running on Windows and Windows is not
currently a well supported platform though we would be happy to change
that.

It is unlikely to require much OS-level tuning, but there are two
potentially important OS-level configurations:

-  File descriptor limits: Kafka uses file descriptors for log segments
   and open connections. If a broker hosts many partitions, consider
   that the broker needs at least
   (number_of_partitions)*(partition_size/segment_size) to track all log
   segments in addition to the number of connections the broker makes.
   We recommend at least 100000 allowed file descriptors for the broker
   processes as a starting point.
-  Max socket buffer size: can be increased to enable high-performance
   data transfer between data centers as `described
   here <http://www.psc.edu/index.php/networking/641-tcp-tune>`__.

`Disks and Filesystem <#diskandfs>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
it doesn't always seem to) because it balances load at a lower level.
The primary downside of RAID is that it is usually a big performance hit
for write throughput and reduces the available disk space.

Another potential benefit of RAID is the ability to tolerate disk
failures. However our experience has been that rebuilding the RAID array
is so I/O intensive that it effectively disables the server, so this
does not provide much real availability improvement.

`Application vs. OS Flush Management <#appvsosflush>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka always immediately writes all data to the filesystem and supports
the ability to configure the flush policy that controls when data is
forced out of the OS cache and onto disk using the flush. This flush
policy can be controlled to force data to disk after a period of time or
after a certain number of messages has been written. There are several
choices in this configuration.

Kafka must eventually call fsync to know that data was flushed. When
recovering from a crash for any log segment not known to be fsync'd
Kafka will check the integrity of each message by checking its CRC and
also rebuild the accompanying offset index file as part of the recovery
process executed on startup.

Note that durability in Kafka does not require syncing data to disk, as
a failed node will always recover from its replicas.

We recommend using the default flush settings which disable application
fsync entirely. This means relying on the background flush done by the
OS and Kafka's own background flush. This provides the best of all
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

In general you don't need to do any low-level tuning of the filesystem,
but in the next few sections we will go over some of this in case it is
useful.

`Understanding Linux OS Flush Behavior <#linuxflush>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Linux, data written to the filesystem is maintained in
`pagecache <http://en.wikipedia.org/wiki/Page_cache>`__ until it must be
written out to disk (due to an application-level fsync or the OS's own
flush policy). The flushing of data is done by a set of background
threads called pdflush (or in post 2.6.32 kernels "flusher threads").

Pdflush has a configurable policy that controls how much dirty data can
be maintained in cache and for how long before it must be written back
to disk. This policy is described
`here <http://web.archive.org/web/20160518040713/http://www.westnet.com/~gsmith/content/linux-pdflush.htm>`__.
When Pdflush cannot keep up with the rate of data being written it will
eventually cause the writing process to block incurring latency in the
writes to slow down the accumulation of data.

You can see the current state of OS memory usage by doing

.. code:: bash

     > cat /proc/meminfo 

The meaning of these values are described in the link above.

Using pagecache has several advantages over an in-process cache for
storing data that will be written out to disk:

-  The I/O scheduler will batch together consecutive small writes into
   bigger physical writes which improves throughput.
-  The I/O scheduler will attempt to re-sequence writes to minimize
   movement of the disk head which improves throughput.
-  It automatically uses all the free memory on the machine

`Filesystem Selection <#filesystems>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka uses regular files on disk, and as such it has no hard dependency
on a specific filesystem. The two filesystems which have the most usage,
however, are EXT4 and XFS. Historically, EXT4 has had more usage, but
recent improvements to the XFS filesystem have shown it to have better
performance characteristics for Kafka's workload with no compromise in
stability.

Comparison testing was performed on a cluster with significant message
loads, using a variety of filesystem creation and mount options. The
primary metric in Kafka that was monitored was the "Request Local Time",
indicating the amount of time append operations were taking. XFS
resulted in much better local times (160ms vs. 250ms+ for the best EXT4
configuration), as well as lower average wait times. The XFS performance
also showed less variability in disk performance.

`General Filesystem Notes <#generalfs>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For any filesystem used for data directories, on Linux systems, the
following options are recommended to be used at mount time:

-  noatime: This option disables updating of a file's atime (last access
   time) attribute when the file is read. This can eliminate a
   significant number of filesystem writes, especially in the case of
   bootstrapping consumers. Kafka does not rely on the atime attributes
   at all, so it is safe to disable this.

`XFS Notes <#xfs>`__
^^^^^^^^^^^^^^^^^^^^

The XFS filesystem has a significant amount of auto-tuning in place, so
it does not require any change in the default settings, either at
filesystem creation time or at mount. The only tuning parameters worth
considering are:

-  largeio: This affects the preferred I/O size reported by the stat
   call. While this can allow for higher performance on larger disk
   writes, in practice it had minimal or no effect on performance.
-  nobarrier: For underlying devices that have battery-backed cache,
   this option can provide a little more performance by disabling
   periodic write flushes. However, if the underlying device is
   well-behaved, it will report to the filesystem that it does not
   require flushes, and this option will have no effect.

`EXT4 Notes <#ext4>`__
^^^^^^^^^^^^^^^^^^^^^^

EXT4 is a serviceable choice of filesystem for the Kafka data
directories, however getting the most performance out of it will require
adjusting several mount options. In addition, these options are
generally unsafe in a failure scenario, and will result in much more
data loss and corruption. For a single broker failure, this is not much
of a concern as the disk can be wiped and the replicas rebuilt from the
cluster. In a multiple-failure scenario, such as a power outage, this
can mean underlying filesystem (and therefore data) corruption that is
not easily recoverable. The following options can be adjusted:

-  data=writeback: Ext4 defaults to data=ordered which puts a strong
   order on some writes. Kafka does not require this ordering as it does
   very paranoid data recovery on all unflushed log. This setting
   removes the ordering constraint and seems to significantly reduce
   latency.
-  Disabling journaling: Journaling is a tradeoff: it makes reboots
   faster after server crashes but it introduces a great deal of
   additional locking which adds variance to write performance. Those
   who don't care about reboot time and want to reduce a major source of
   write latency spikes can turn off journaling entirely.
-  commit=num_secs: This tunes the frequency with which ext4 commits to
   its metadata journal. Setting this to a lower value reduces the loss
   of unflushed data during a crash. Setting this to a higher value will
   improve throughput.
-  nobh: This setting controls additional ordering guarantees when using
   data=writeback mode. This should be safe with Kafka as we do not
   depend on write ordering and improves throughput and latency.
-  delalloc: Delayed allocation means that the filesystem avoid
   allocating any blocks until the physical write occurs. This allows
   ext4 to allocate a large extent instead of smaller pages and helps
   ensure the data is written sequentially. This feature is great for
   throughput. It does seem to involve some locking in the filesystem
   which adds a bit of latency variance.

`6.6 Monitoring <#monitoring>`__
--------------------------------

Kafka uses Yammer Metrics for metrics reporting in the server and Scala
clients. The Java clients use Kafka Metrics, a built-in metrics registry
that minimizes transitive dependencies pulled into client applications.
Both expose metrics via JMX and can be configured to report stats using
pluggable stats reporters to hook up to your monitoring system.

All Kafka rate metrics have a corresponding cumulative count metric with
suffix ``-total``. For example, ``records-consumed-rate`` has a
corresponding metric named ``records-consumed-total``.

The easiest way to see the available metrics is to fire up jconsole and
point it at a running kafka client or server; this will allow browsing
all metrics with JMX.

We do graphing and alerting on the following metrics:

+-----------------------+-----------------------+-----------------------+
| Description           | Mbean name            | Normal value          |
+=======================+=======================+=======================+
| Message in rate       | kafka.server:type=Bro |                       |
|                       | kerTopicMetrics,name= |                       |
|                       | MessagesInPerSec      |                       |
+-----------------------+-----------------------+-----------------------+
| Byte in rate          | kafka.server:type=Bro |                       |
|                       | kerTopicMetrics,name= |                       |
|                       | BytesInPerSec         |                       |
+-----------------------+-----------------------+-----------------------+
| Request rate          | kafka.network:type=Re |                       |
|                       | questMetrics,name=Req |                       |
|                       | uestsPerSec,request={ |                       |
|                       | Produce|FetchConsumer |                       |
|                       | |FetchFollower}       |                       |
+-----------------------+-----------------------+-----------------------+
| Error rate            | kafka.network:type=Re | Number of errors in   |
|                       | questMetrics,name=Err | responses counted     |
|                       | orsPerSec,request=([- | per-request-type,     |
|                       | .\w]+),error=([-.\w]+ | per-error-code. If a  |
|                       | )                     | response contains     |
|                       |                       | multiple errors, all  |
|                       |                       | are counted.          |
|                       |                       | error=NONE indicates  |
|                       |                       | successful responses. |
+-----------------------+-----------------------+-----------------------+
| Request size in bytes | kafka.network:type=Re | Size of requests for  |
|                       | questMetrics,name=Req | each request type.    |
|                       | uestBytes,request=([- |                       |
|                       | .\w]+)                |                       |
+-----------------------+-----------------------+-----------------------+
| Temporary memory size | kafka.network:type=Re | Temporary memory used |
| in bytes              | questMetrics,name=Tem | for message format    |
|                       | poraryMemoryBytes,req | conversions and       |
|                       | uest={Produce|Fetch}  | decompression.        |
+-----------------------+-----------------------+-----------------------+
| Message conversion    | kafka.network:type=Re | Time in milliseconds  |
| time                  | questMetrics,name=Mes | spent on message      |
|                       | sageConversionsTimeMs | format conversions.   |
|                       | ,request={Produce|Fet |                       |
|                       | ch}                   |                       |
+-----------------------+-----------------------+-----------------------+
| Message conversion    | kafka.server:type=Bro | Number of records     |
| rate                  | kerTopicMetrics,name= | which required        |
|                       | {Produce|Fetch}Messag | message format        |
|                       | eConversionsPerSec,to | conversion.           |
|                       | pic=([-.\w]+)         |                       |
+-----------------------+-----------------------+-----------------------+
| Byte out rate         | kafka.server:type=Bro |                       |
|                       | kerTopicMetrics,name= |                       |
|                       | BytesOutPerSec        |                       |
+-----------------------+-----------------------+-----------------------+
| Log flush rate and    | kafka.log:type=LogFlu |                       |
| time                  | shStats,name=LogFlush |                       |
|                       | RateAndTimeMs         |                       |
+-----------------------+-----------------------+-----------------------+
| # of under replicated | kafka.server:type=Rep | 0                     |
| partitions (|ISR\| <  | licaManager,name=Unde |                       |
| \|all replicas|)      | rReplicatedPartitions |                       |
+-----------------------+-----------------------+-----------------------+
| # of under minIsr     | kafka.server:type=Rep | 0                     |
| partitions (|ISR\| <  | licaManager,name=Unde |                       |
| min.insync.replicas)  | rMinIsrPartitionCount |                       |
+-----------------------+-----------------------+-----------------------+
| # of offline log      | kafka.log:type=LogMan | 0                     |
| directories           | ager,name=OfflineLogD |                       |
|                       | irectoryCount         |                       |
+-----------------------+-----------------------+-----------------------+
| Is controller active  | kafka.controller:type | only one broker in    |
| on broker             | =KafkaController,name | the cluster should    |
|                       | =ActiveControllerCoun | have 1                |
|                       | t                     |                       |
+-----------------------+-----------------------+-----------------------+
| Leader election rate  | kafka.controller:type | non-zero when there   |
|                       | =ControllerStats,name | are broker failures   |
|                       | =LeaderElectionRateAn |                       |
|                       | dTimeMs               |                       |
+-----------------------+-----------------------+-----------------------+
| Unclean leader        | kafka.controller:type | 0                     |
| election rate         | =ControllerStats,name |                       |
|                       | =UncleanLeaderElectio |                       |
|                       | nsPerSec              |                       |
+-----------------------+-----------------------+-----------------------+
| Partition counts      | kafka.server:type=Rep | mostly even across    |
|                       | licaManager,name=Part | brokers               |
|                       | itionCount            |                       |
+-----------------------+-----------------------+-----------------------+
| Leader replica counts | kafka.server:type=Rep | mostly even across    |
|                       | licaManager,name=Lead | brokers               |
|                       | erCount               |                       |
+-----------------------+-----------------------+-----------------------+
| ISR shrink rate       | kafka.server:type=Rep | If a broker goes      |
|                       | licaManager,name=IsrS | down, ISR for some of |
|                       | hrinksPerSec          | the partitions will   |
|                       |                       | shrink. When that     |
|                       |                       | broker is up again,   |
|                       |                       | ISR will be expanded  |
|                       |                       | once the replicas are |
|                       |                       | fully caught up.      |
|                       |                       | Other than that, the  |
|                       |                       | expected value for    |
|                       |                       | both ISR shrink rate  |
|                       |                       | and expansion rate is |
|                       |                       | 0.                    |
+-----------------------+-----------------------+-----------------------+
| ISR expansion rate    | kafka.server:type=Rep | See above             |
|                       | licaManager,name=IsrE |                       |
|                       | xpandsPerSec          |                       |
+-----------------------+-----------------------+-----------------------+
| Max lag in messages   | kafka.server:type=Rep | lag should be         |
| btw follower and      | licaFetcherManager,na | proportional to the   |
| leader replicas       | me=MaxLag,clientId=Re | maximum batch size of |
|                       | plica                 | a produce request.    |
+-----------------------+-----------------------+-----------------------+
| Lag in messages per   | kafka.server:type=Fet | lag should be         |
| follower replica      | cherLagMetrics,name=C | proportional to the   |
|                       | onsumerLag,clientId=( | maximum batch size of |
|                       | [-.\w]+),topic=([-.\w | a produce request.    |
|                       | ]+),partition=([0-9]+ |                       |
|                       | )                     |                       |
+-----------------------+-----------------------+-----------------------+
| Requests waiting in   | kafka.server:type=Del | non-zero if ack=-1 is |
| the producer          | ayedOperationPurgator | used                  |
| purgatory             | y,name=PurgatorySize, |                       |
|                       | delayedOperation=Prod |                       |
|                       | uce                   |                       |
+-----------------------+-----------------------+-----------------------+
| Requests waiting in   | kafka.server:type=Del | size depends on       |
| the fetch purgatory   | ayedOperationPurgator | fetch.wait.max.ms in  |
|                       | y,name=PurgatorySize, | the consumer          |
|                       | delayedOperation=Fetc |                       |
|                       | h                     |                       |
+-----------------------+-----------------------+-----------------------+
| Request total time    | kafka.network:type=Re | broken into queue,    |
|                       | questMetrics,name=Tot | local, remote and     |
|                       | alTimeMs,request={Pro | response send time    |
|                       | duce|FetchConsumer|Fe |                       |
|                       | tchFollower}          |                       |
+-----------------------+-----------------------+-----------------------+
| Time the request      | kafka.network:type=Re |                       |
| waits in the request  | questMetrics,name=Req |                       |
| queue                 | uestQueueTimeMs,reque |                       |
|                       | st={Produce|FetchCons |                       |
|                       | umer|FetchFollower}   |                       |
+-----------------------+-----------------------+-----------------------+
| Time the request is   | kafka.network:type=Re |                       |
| processed at the      | questMetrics,name=Loc |                       |
| leader                | alTimeMs,request={Pro |                       |
|                       | duce|FetchConsumer|Fe |                       |
|                       | tchFollower}          |                       |
+-----------------------+-----------------------+-----------------------+
| Time the request      | kafka.network:type=Re | non-zero for produce  |
| waits for the         | questMetrics,name=Rem | requests when ack=-1  |
| follower              | oteTimeMs,request={Pr |                       |
|                       | oduce|FetchConsumer|F |                       |
|                       | etchFollower}         |                       |
+-----------------------+-----------------------+-----------------------+
| Time the request      | kafka.network:type=Re |                       |
| waits in the response | questMetrics,name=Res |                       |
| queue                 | ponseQueueTimeMs,requ |                       |
|                       | est={Produce|FetchCon |                       |
|                       | sumer|FetchFollower}  |                       |
+-----------------------+-----------------------+-----------------------+
| Time to send the      | kafka.network:type=Re |                       |
| response              | questMetrics,name=Res |                       |
|                       | ponseSendTimeMs,reque |                       |
|                       | st={Produce|FetchCons |                       |
|                       | umer|FetchFollower}   |                       |
+-----------------------+-----------------------+-----------------------+
| Number of messages    | *Old consumer:*       |                       |
| the consumer lags     | kafka.consumer:type=C |                       |
| behind the producer   | onsumerFetcherManager |                       |
| by. Published by the  | ,name=MaxLag,clientId |                       |
| consumer, not broker. | =([-.\w]+)            |                       |
|                       |                       |                       |
|                       | *New consumer:*       |                       |
|                       | kafka.consumer:type=c |                       |
|                       | onsumer-fetch-manager |                       |
|                       | -metrics,client-id={c |                       |
|                       | lient-id}             |                       |
|                       | Attribute:            |                       |
|                       | records-lag-max       |                       |
+-----------------------+-----------------------+-----------------------+
| The average fraction  | kafka.network:type=So | between 0 and 1,      |
| of time the network   | cketServer,name=Netwo | ideally > 0.3         |
| processors are idle   | rkProcessorAvgIdlePer |                       |
|                       | cent                  |                       |
+-----------------------+-----------------------+-----------------------+
| The average fraction  | kafka.server:type=Kaf | between 0 and 1,      |
| of time the request   | kaRequestHandlerPool, | ideally > 0.3         |
| handler threads are   | name=RequestHandlerAv |                       |
| idle                  | gIdlePercent          |                       |
+-----------------------+-----------------------+-----------------------+
| Bandwidth quota       | kafka.server:type={Pr | Two attributes.       |
| metrics per (user,    | oduce|Fetch},user=([- | throttle-time         |
| client-id), user or   | .\w]+),client-id=([-. | indicates the amount  |
| client-id             | \w]+)                 | of time in ms the     |
|                       |                       | client was throttled. |
|                       |                       | Ideally = 0.          |
|                       |                       | byte-rate indicates   |
|                       |                       | the data              |
|                       |                       | produce/consume rate  |
|                       |                       | of the client in      |
|                       |                       | bytes/sec. For (user, |
|                       |                       | client-id) quotas,    |
|                       |                       | both user and         |
|                       |                       | client-id are         |
|                       |                       | specified. If         |
|                       |                       | per-client-id quota   |
|                       |                       | is applied to the     |
|                       |                       | client, user is not   |
|                       |                       | specified. If         |
|                       |                       | per-user quota is     |
|                       |                       | applied, client-id is |
|                       |                       | not specified.        |
+-----------------------+-----------------------+-----------------------+
| Request quota metrics | kafka.server:type=Req | Two attributes.       |
| per (user,            | uest,user=([-.\w]+),c | throttle-time         |
| client-id), user or   | lient-id=([-.\w]+)    | indicates the amount  |
| client-id             |                       | of time in ms the     |
|                       |                       | client was throttled. |
|                       |                       | Ideally = 0.          |
|                       |                       | request-time          |
|                       |                       | indicates the         |
|                       |                       | percentage of time    |
|                       |                       | spent in broker       |
|                       |                       | network and I/O       |
|                       |                       | threads to process    |
|                       |                       | requests from client  |
|                       |                       | group. For (user,     |
|                       |                       | client-id) quotas,    |
|                       |                       | both user and         |
|                       |                       | client-id are         |
|                       |                       | specified. If         |
|                       |                       | per-client-id quota   |
|                       |                       | is applied to the     |
|                       |                       | client, user is not   |
|                       |                       | specified. If         |
|                       |                       | per-user quota is     |
|                       |                       | applied, client-id is |
|                       |                       | not specified.        |
+-----------------------+-----------------------+-----------------------+
| Requests exempt from  | kafka.server:type=Req | exempt-throttle-time  |
| throttling            | uest                  | indicates the         |
|                       |                       | percentage of time    |
|                       |                       | spent in broker       |
|                       |                       | network and I/O       |
|                       |                       | threads to process    |
|                       |                       | requests that are     |
|                       |                       | exempt from           |
|                       |                       | throttling.           |
+-----------------------+-----------------------+-----------------------+
| ZooKeeper client      | kafka.server:type=Zoo | Latency in            |
| request latency       | KeeperClientMetrics,n | millseconds for       |
|                       | ame=ZooKeeperRequestL | ZooKeeper requests    |
|                       | atencyMs              | from broker.          |
+-----------------------+-----------------------+-----------------------+
| ZooKeeper connection  | kafka.server:type=Ses | Connection status of  |
| status                | sionExpireListener,na | broker's ZooKeeper    |
|                       | me=SessionState       | session which may be  |
|                       |                       | one of                |
|                       |                       | Disconnected|SyncConn |
|                       |                       | ected|AuthFailed|Conn |
|                       |                       | ectedReadOnly|SaslAut |
|                       |                       | henticated|Expired.   |
+-----------------------+-----------------------+-----------------------+

`Common monitoring metrics for producer/consumer/connect/streams <#selector_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following metrics are available on
producer/consumer/connector/streams instances. For specific metrics,
please see following sections.

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| connection-close-rate | Connections closed    | kafka.[producer|consu |
|                       | per second in the     | mer|connect]:type=[pr |
|                       | window.               | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| connection-creation-r | New connections       | kafka.[producer|consu |
| ate                   | established per       | mer|connect]:type=[pr |
|                       | second in the window. | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| network-io-rate       | The average number of | kafka.[producer|consu |
|                       | network operations    | mer|connect]:type=[pr |
|                       | (reads or writes) on  | oducer|consumer|conne |
|                       | all connections per   | ct]-metrics,client-id |
|                       | second.               | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| outgoing-byte-rate    | The average number of | kafka.[producer|consu |
|                       | outgoing bytes sent   | mer|connect]:type=[pr |
|                       | per second to all     | oducer|consumer|conne |
|                       | servers.              | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| request-rate          | The average number of | kafka.[producer|consu |
|                       | requests sent per     | mer|connect]:type=[pr |
|                       | second.               | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| request-size-avg      | The average size of   | kafka.[producer|consu |
|                       | all requests in the   | mer|connect]:type=[pr |
|                       | window.               | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| request-size-max      | The maximum size of   | kafka.[producer|consu |
|                       | any request sent in   | mer|connect]:type=[pr |
|                       | the window.           | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| incoming-byte-rate    | Bytes/second read off | kafka.[producer|consu |
|                       | all sockets.          | mer|connect]:type=[pr |
|                       |                       | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| response-rate         | Responses received    | kafka.[producer|consu |
|                       | sent per second.      | mer|connect]:type=[pr |
|                       |                       | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| select-rate           | Number of times the   | kafka.[producer|consu |
|                       | I/O layer checked for | mer|connect]:type=[pr |
|                       | new I/O to perform    | oducer|consumer|conne |
|                       | per second.           | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| io-wait-time-ns-avg   | The average length of | kafka.[producer|consu |
|                       | time the I/O thread   | mer|connect]:type=[pr |
|                       | spent waiting for a   | oducer|consumer|conne |
|                       | socket ready for      | ct]-metrics,client-id |
|                       | reads or writes in    | =([-.\w]+)            |
|                       | nanoseconds.          |                       |
+-----------------------+-----------------------+-----------------------+
| io-wait-ratio         | The fraction of time  | kafka.[producer|consu |
|                       | the I/O thread spent  | mer|connect]:type=[pr |
|                       | waiting.              | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| io-time-ns-avg        | The average length of | kafka.[producer|consu |
|                       | time for I/O per      | mer|connect]:type=[pr |
|                       | select call in        | oducer|consumer|conne |
|                       | nanoseconds.          | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| io-ratio              | The fraction of time  | kafka.[producer|consu |
|                       | the I/O thread spent  | mer|connect]:type=[pr |
|                       | doing I/O.            | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| connection-count      | The current number of | kafka.[producer|consu |
|                       | active connections.   | mer|connect]:type=[pr |
|                       |                       | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| successful-authentica | Connections that were | kafka.[producer|consu |
| tion-rate             | successfully          | mer|connect]:type=[pr |
|                       | authenticated using   | oducer|consumer|conne |
|                       | SASL or SSL.          | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+
| failed-authentication | Connections that      | kafka.[producer|consu |
| -rate                 | failed                | mer|connect]:type=[pr |
|                       | authentication.       | oducer|consumer|conne |
|                       |                       | ct]-metrics,client-id |
|                       |                       | =([-.\w]+)            |
+-----------------------+-----------------------+-----------------------+

`Common Per-broker metrics for producer/consumer/connect/streams <#common_node_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following metrics are available on
producer/consumer/connector/streams instances. For specific metrics,
please see following sections.

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| outgoing-byte-rate    | The average number of | kafka.producer:type=[ |
|                       | outgoing bytes sent   | consumer|producer|con |
|                       | per second for a      | nect]-node-metrics,cl |
|                       | node.                 | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| request-rate          | The average number of | kafka.producer:type=[ |
|                       | requests sent per     | consumer|producer|con |
|                       | second for a node.    | nect]-node-metrics,cl |
|                       |                       | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| request-size-avg      | The average size of   | kafka.producer:type=[ |
|                       | all requests in the   | consumer|producer|con |
|                       | window for a node.    | nect]-node-metrics,cl |
|                       |                       | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| request-size-max      | The maximum size of   | kafka.producer:type=[ |
|                       | any request sent in   | consumer|producer|con |
|                       | the window for a      | nect]-node-metrics,cl |
|                       | node.                 | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| incoming-byte-rate    | The average number of | kafka.producer:type=[ |
|                       | responses received    | consumer|producer|con |
|                       | per second for a      | nect]-node-metrics,cl |
|                       | node.                 | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| request-latency-avg   | The average request   | kafka.producer:type=[ |
|                       | latency in ms for a   | consumer|producer|con |
|                       | node.                 | nect]-node-metrics,cl |
|                       |                       | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| request-latency-max   | The maximum request   | kafka.producer:type=[ |
|                       | latency in ms for a   | consumer|producer|con |
|                       | node.                 | nect]-node-metrics,cl |
|                       |                       | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+
| response-rate         | Responses received    | kafka.producer:type=[ |
|                       | sent per second for a | consumer|producer|con |
|                       | node.                 | nect]-node-metrics,cl |
|                       |                       | ient-id=([-.\w]+),nod |
|                       |                       | e-id=([0-9]+)         |
+-----------------------+-----------------------+-----------------------+

`Producer monitoring <#producer_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following metrics are available on producer instances.

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| waiting-threads       | The number of user    | kafka.producer:type=p |
|                       | threads blocked       | roducer-metrics,clien |
|                       | waiting for buffer    | t-id=([-.\w]+)        |
|                       | memory to enqueue     |                       |
|                       | their records.        |                       |
+-----------------------+-----------------------+-----------------------+
| buffer-total-bytes    | The maximum amount of | kafka.producer:type=p |
|                       | buffer memory the     | roducer-metrics,clien |
|                       | client can use        | t-id=([-.\w]+)        |
|                       | (whether or not it is |                       |
|                       | currently used).      |                       |
+-----------------------+-----------------------+-----------------------+
| buffer-available-byte | The total amount of   | kafka.producer:type=p |
| s                     | buffer memory that is | roducer-metrics,clien |
|                       | not being used        | t-id=([-.\w]+)        |
|                       | (either unallocated   |                       |
|                       | or in the free list). |                       |
+-----------------------+-----------------------+-----------------------+
| bufferpool-wait-time  | The fraction of time  | kafka.producer:type=p |
|                       | an appender waits for | roducer-metrics,clien |
|                       | space allocation.     | t-id=([-.\w]+)        |
+-----------------------+-----------------------+-----------------------+

`Producer Sender Metrics <#producer_sender_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`New consumer monitoring <#new_consumer_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following metrics are available on new consumer instances.

`Consumer Group Metrics <#new_consumer_group_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| commit-latency-avg    | The average time      | kafka.consumer:type=c |
|                       | taken for a commit    | onsumer-coordinator-m |
|                       | request               | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| commit-latency-max    | The max time taken    | kafka.consumer:type=c |
|                       | for a commit request  | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| commit-rate           | The number of commit  | kafka.consumer:type=c |
|                       | calls per second      | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| assigned-partitions   | The number of         | kafka.consumer:type=c |
|                       | partitions currently  | onsumer-coordinator-m |
|                       | assigned to this      | etrics,client-id=([-. |
|                       | consumer              | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| heartbeat-response-ti | The max time taken to | kafka.consumer:type=c |
| me-max                | receive a response to | onsumer-coordinator-m |
|                       | a heartbeat request   | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| heartbeat-rate        | The average number of | kafka.consumer:type=c |
|                       | heartbeats per second | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| join-time-avg         | The average time      | kafka.consumer:type=c |
|                       | taken for a group     | onsumer-coordinator-m |
|                       | rejoin                | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| join-time-max         | The max time taken    | kafka.consumer:type=c |
|                       | for a group rejoin    | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| join-rate             | The number of group   | kafka.consumer:type=c |
|                       | joins per second      | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| sync-time-avg         | The average time      | kafka.consumer:type=c |
|                       | taken for a group     | onsumer-coordinator-m |
|                       | sync                  | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| sync-time-max         | The max time taken    | kafka.consumer:type=c |
|                       | for a group sync      | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| sync-rate             | The number of group   | kafka.consumer:type=c |
|                       | syncs per second      | onsumer-coordinator-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| last-heartbeat-second | The number of seconds | kafka.consumer:type=c |
| s-ago                 | since the last        | onsumer-coordinator-m |
|                       | controller heartbeat  | etrics,client-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+

`Consumer Fetch Metrics <#new_consumer_fetch_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`Connect Monitoring <#connect_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Connect worker process contains all the producer and consumer metrics
as well as metrics specific to Connect. The worker process itself has a
number of metrics, while each connector and task have additional
metrics.

`Streams Monitoring <#kafka_streams_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Kafka Streams instance contains all the producer and consumer metrics
as well as additional metrics specific to streams. By default Kafka
Streams has metrics with two recording levels: debug and info. The debug
level records all metrics, while the info level records only the
thread-level metrics.

Note that the metrics have a 3-layer hierarchy. At the top level there
are per-thread metrics. Each thread has tasks, with their own metrics.
Each task has a number of processor nodes, with their own metrics. Each
task also has a number of state stores and record caches, all with their
own metrics.

Use the following configuration option to specify which metrics you want
collected:

::

    metrics.recording.level="info"

`Thread Metrics <#kafka_streams_thread_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All the following metrics have a recording level of \``info``:

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| commit-latency-avg    | The average execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | committing, across    | d=([-.\w]+)           |
|                       | all running tasks of  |                       |
|                       | this thread.          |                       |
+-----------------------+-----------------------+-----------------------+
| commit-latency-max    | The maximum execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | committing across all | d=([-.\w]+)           |
|                       | running tasks of this |                       |
|                       | thread.               |                       |
+-----------------------+-----------------------+-----------------------+
| poll-latency-avg      | The average execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | polling, across all   | d=([-.\w]+)           |
|                       | running tasks of this |                       |
|                       | thread.               |                       |
+-----------------------+-----------------------+-----------------------+
| poll-latency-max      | The maximum execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | polling across all    | d=([-.\w]+)           |
|                       | running tasks of this |                       |
|                       | thread.               |                       |
+-----------------------+-----------------------+-----------------------+
| process-latency-avg   | The average execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | processing, across    | d=([-.\w]+)           |
|                       | all running tasks of  |                       |
|                       | this thread.          |                       |
+-----------------------+-----------------------+-----------------------+
| process-latency-max   | The maximum execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | processing across all | d=([-.\w]+)           |
|                       | running tasks of this |                       |
|                       | thread.               |                       |
+-----------------------+-----------------------+-----------------------+
| punctuate-latency-avg | The average execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | punctuating, across   | d=([-.\w]+)           |
|                       | all running tasks of  |                       |
|                       | this thread.          |                       |
+-----------------------+-----------------------+-----------------------+
| punctuate-latency-max | The maximum execution | kafka.streams:type=st |
|                       | time in ms for        | ream-metrics,client-i |
|                       | punctuating across    | d=([-.\w]+)           |
|                       | all running tasks of  |                       |
|                       | this thread.          |                       |
+-----------------------+-----------------------+-----------------------+
| commit-rate           | The average number of | kafka.streams:type=st |
|                       | commits per second    | ream-metrics,client-i |
|                       | across all tasks.     | d=([-.\w]+)           |
+-----------------------+-----------------------+-----------------------+
| poll-rate             | The average number of | kafka.streams:type=st |
|                       | polls per second      | ream-metrics,client-i |
|                       | across all tasks.     | d=([-.\w]+)           |
+-----------------------+-----------------------+-----------------------+
| process-rate          | The average number of | kafka.streams:type=st |
|                       | process calls per     | ream-metrics,client-i |
|                       | second across all     | d=([-.\w]+)           |
|                       | tasks.                |                       |
+-----------------------+-----------------------+-----------------------+
| punctuate-rate        | The average number of | kafka.streams:type=st |
|                       | punctuates per second | ream-metrics,client-i |
|                       | across all tasks.     | d=([-.\w]+)           |
+-----------------------+-----------------------+-----------------------+
| task-created-rate     | The average number of | kafka.streams:type=st |
|                       | newly created tasks   | ream-metrics,client-i |
|                       | per second.           | d=([-.\w]+)           |
+-----------------------+-----------------------+-----------------------+
| task-closed-rate      | The average number of | kafka.streams:type=st |
|                       | tasks closed per      | ream-metrics,client-i |
|                       | second.               | d=([-.\w]+)           |
+-----------------------+-----------------------+-----------------------+
| skipped-records-rate  | The average number of | kafka.streams:type=st |
|                       | skipped records per   | ream-metrics,client-i |
|                       | second.               | d=([-.\w]+)           |
+-----------------------+-----------------------+-----------------------+

`Task Metrics <#kafka_streams_task_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All the following metrics have a recording level of \``debug``:

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| commit-latency-avg    | The average commit    | kafka.streams:type=st |
|                       | time in ns for this   | ream-task-metrics,cli |
|                       | task.                 | ent-id=([-.\w]+),task |
|                       |                       | -id=([-.\w]+)         |
+-----------------------+-----------------------+-----------------------+
| commit-latency-max    | The maximum commit    | kafka.streams:type=st |
|                       | time in ns for this   | ream-task-metrics,cli |
|                       | task.                 | ent-id=([-.\w]+),task |
|                       |                       | -id=([-.\w]+)         |
+-----------------------+-----------------------+-----------------------+
| commit-rate           | The average number of | kafka.streams:type=st |
|                       | commit calls per      | ream-task-metrics,cli |
|                       | second.               | ent-id=([-.\w]+),task |
|                       |                       | -id=([-.\w]+)         |
+-----------------------+-----------------------+-----------------------+

`Processor Node Metrics <#kafka_streams_node_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All the following metrics have a recording level of \``debug``:

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| process-latency-avg   | The average process   | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| process-latency-max   | The maximum process   | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| punctuate-latency-avg | The average punctuate | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| punctuate-latency-max | The maximum punctuate | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| create-latency-avg    | The average create    | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| create-latency-max    | The maximum create    | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| destroy-latency-avg   | The average destroy   | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| destroy-latency-max   | The maximum destroy   | kafka.streams:type=st |
|                       | execution time in ns. | ream-processor-node-m |
|                       |                       | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| process-rate          | The average number of | kafka.streams:type=st |
|                       | process operations    | ream-processor-node-m |
|                       | per second.           | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| punctuate-rate        | The average number of | kafka.streams:type=st |
|                       | punctuate operations  | ream-processor-node-m |
|                       | per second.           | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| create-rate           | The average number of | kafka.streams:type=st |
|                       | create operations per | ream-processor-node-m |
|                       | second.               | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| destroy-rate          | The average number of | kafka.streams:type=st |
|                       | destroy operations    | ream-processor-node-m |
|                       | per second.           | etrics,client-id=([-. |
|                       |                       | \w]+),task-id=([-.\w] |
|                       |                       | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+
| forward-rate          | The average rate of   | kafka.streams:type=st |
|                       | records being         | ream-processor-node-m |
|                       | forwarded downstream, | etrics,client-id=([-. |
|                       | from source nodes     | \w]+),task-id=([-.\w] |
|                       | only, per second.     | +),processor-node-id= |
|                       |                       | ([-.\w]+)             |
+-----------------------+-----------------------+-----------------------+

`State Store Metrics <#kafka_streams_store_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All the following metrics have a recording level of \``debug``:

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| put-latency-avg       | The average put       | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-latency-max       | The maximum put       | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-if-absent-latency | The average           | kafka.streams:type=st |
| -avg                  | put-if-absent         | ream-[store-type]-sta |
|                       | execution time in ns. | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-if-absent-latency | The maximum           | kafka.streams:type=st |
| -max                  | put-if-absent         | ream-[store-type]-sta |
|                       | execution time in ns. | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| get-latency-avg       | The average get       | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| get-latency-max       | The maximum get       | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| delete-latency-avg    | The average delete    | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| delete-latency-max    | The maximum delete    | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-all-latency-avg   | The average put-all   | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-all-latency-max   | The maximum put-all   | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| all-latency-avg       | The average all       | kafka.streams:type=st |
|                       | operation execution   | ream-[store-type]-sta |
|                       | time in ns.           | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| all-latency-max       | The maximum all       | kafka.streams:type=st |
|                       | operation execution   | ream-[store-type]-sta |
|                       | time in ns.           | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| range-latency-avg     | The average range     | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| range-latency-max     | The maximum range     | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| flush-latency-avg     | The average flush     | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| flush-latency-max     | The maximum flush     | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| restore-latency-avg   | The average restore   | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| restore-latency-max   | The maximum restore   | kafka.streams:type=st |
|                       | execution time in ns. | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-rate              | The average put rate  | kafka.streams:type=st |
|                       | for this store.       | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-if-absent-rate    | The average           | kafka.streams:type=st |
|                       | put-if-absent rate    | ream-[store-type]-sta |
|                       | for this store.       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| get-rate              | The average get rate  | kafka.streams:type=st |
|                       | for this store.       | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| delete-rate           | The average delete    | kafka.streams:type=st |
|                       | rate for this store.  | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| put-all-rate          | The average put-all   | kafka.streams:type=st |
|                       | rate for this store.  | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| all-rate              | The average all       | kafka.streams:type=st |
|                       | operation rate for    | ream-[store-type]-sta |
|                       | this store.           | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| range-rate            | The average range     | kafka.streams:type=st |
|                       | rate for this store.  | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| flush-rate            | The average flush     | kafka.streams:type=st |
|                       | rate for this store.  | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+
| restore-rate          | The average restore   | kafka.streams:type=st |
|                       | rate for this store.  | ream-[store-type]-sta |
|                       |                       | te-metrics,client-id= |
|                       |                       | ([-.\w]+),task-id=([- |
|                       |                       | .\w]+),[store-type]-s |
|                       |                       | tate-id=([-.\w]+)     |
+-----------------------+-----------------------+-----------------------+

`Record Cache Metrics <#kafka_streams_cache_monitoring>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All the following metrics have a recording level of \``debug``:

+-----------------------+-----------------------+-----------------------+
| Metric/Attribute name | Description           | Mbean name            |
+=======================+=======================+=======================+
| hitRatio-avg          | The average cache hit | kafka.streams:type=st |
|                       | ratio defined as the  | ream-record-cache-met |
|                       | ratio of cache read   | rics,client-id=([-.\w |
|                       | hits over the total   | ]+),task-id=([-.\w]+) |
|                       | cache read requests.  | ,record-cache-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| hitRatio-min          | The mininum cache hit | kafka.streams:type=st |
|                       | ratio.                | ream-record-cache-met |
|                       |                       | rics,client-id=([-.\w |
|                       |                       | ]+),task-id=([-.\w]+) |
|                       |                       | ,record-cache-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+
| hitRatio-max          | The maximum cache hit | kafka.streams:type=st |
|                       | ratio.                | ream-record-cache-met |
|                       |                       | rics,client-id=([-.\w |
|                       |                       | ]+),task-id=([-.\w]+) |
|                       |                       | ,record-cache-id=([-. |
|                       |                       | \w]+)                 |
+-----------------------+-----------------------+-----------------------+

`Others <#others_monitoring>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We recommend monitoring GC time and other stats and various server stats
such as CPU utilization, I/O service time, etc. On the client side, we
recommend monitoring the message/byte rate (global and per topic),
request rate/size/time, and on the consumer side, max lag in messages
among all partitions and min fetch request rate. For a consumer to keep
up, max lag needs to be less than a threshold and min fetch rate needs
to be larger than 0.

`Audit <#basic_ops_audit>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The final alerting we do is on the correctness of the data delivery. We
audit that every message that is sent is consumed by all consumers and
measure the lag for this to occur. For important topics we alert if a
certain completeness is not achieved in a certain time period. The
details of this are discussed in KAFKA-260.

`6.7 ZooKeeper <#zk>`__
-----------------------

`Stable version <#zkversion>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The current stable branch is 3.4 and the latest release of that branch
is 3.4.9.

`Operationalizing ZooKeeper <#zkops>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Operationally, we do the following for a healthy ZooKeeper installation:

-  Redundancy in the physical/hardware/network layout: try not to put
   them all in the same rack, decent (but don't go nuts) hardware, try
   to keep redundant power and network paths, etc. A typical ZooKeeper
   ensemble has 5 or 7 servers, which tolerates 2 and 3 servers down,
   respectively. If you have a small deployment, then using 3 servers is
   acceptable, but keep in mind that you'll only be able to tolerate 1
   server down in this case.
-  I/O segregation: if you do a lot of write type traffic you'll almost
   definitely want the transaction logs on a dedicated disk group.
   Writes to the transaction log are synchronous (but batched for
   performance), and consequently, concurrent writes can significantly
   affect performance. ZooKeeper snapshots can be one such a source of
   concurrent writes, and ideally should be written on a disk group
   separate from the transaction log. Snapshots are written to disk
   asynchronously, so it is typically ok to share with the operating
   system and message log files. You can configure a server to use a
   separate disk group with the dataLogDir parameter.
-  Application segregation: Unless you really understand the application
   patterns of other apps that you want to install on the same box, it
   can be a good idea to run ZooKeeper in isolation (though this can be
   a balancing act with the capabilities of the hardware).
-  Use care with virtualization: It can work, depending on your cluster
   layout and read/write patterns and SLAs, but the tiny overheads
   introduced by the virtualization layer can add up and throw off
   ZooKeeper, as it can be very time sensitive
-  ZooKeeper configuration: It's java, make sure you give it 'enough'
   heap space (We usually run them with 3-5G, but that's mostly due to
   the data set size we have here). Unfortunately we don't have a good
   formula for it, but keep in mind that allowing for more ZooKeeper
   state means that snapshots can become large, and large snapshots
   affect recovery time. In fact, if the snapshot becomes too large (a
   few gigabytes), then you may need to increase the initLimit parameter
   to give enough time for servers to recover and join the ensemble.
-  Monitoring: Both JMX and the 4 letter words (4lw) commands are very
   useful, they do overlap in some cases (and in those cases we prefer
   the 4 letter commands, they seem more predictable, or at the very
   least, they work better with the LI monitoring infrastructure)
-  Don't overbuild the cluster: large clusters, especially in a write
   heavy usage pattern, means a lot of intracluster communication
   (quorums on the writes and subsequent cluster member updates), but
   don't underbuild it (and risk swamping the cluster). Having more
   servers adds to your read capacity.

Overall, we try to keep the ZooKeeper system as small as will handle the
load (plus standard growth capacity planning) and as simple as possible.
We try not to do anything fancy with the configuration or application
layout as compared to the official release as well as keep it as self
contained as possible. For these reasons, we tend to skip the OS
packaged versions, since it has a tendency to try to put things in the
OS standard hierarchy, which can be 'messy', for want of a better way to
word it.

