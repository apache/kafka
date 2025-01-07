# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition, quorum, consumer_group

import signal


class OffsetValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 1

    def __init__(self, test_context):
        super(OffsetValidationTest, self).__init__(test_context, num_consumers=3, num_producers=1,
                                                     num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 2 }
        })

    def rolling_bounce_consumers(self, consumer, keep_alive=0, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes[keep_alive:]:
                consumer.stop_node(node, clean_shutdown)

                wait_until(lambda: len(consumer.dead_nodes()) == 1,
                           timeout_sec=60,
                           err_msg="Timed out waiting for the consumer to shutdown")

                consumer.start_node(node)

                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def bounce_all_consumers(self, consumer, keep_alive=0, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes[keep_alive:]:
                consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers - keep_alive, timeout_sec=10,
                       err_msg="Timed out waiting for the consumers to shutdown")

            for node in consumer.nodes[keep_alive:]:
                consumer.start_node(node)

            self.await_all_members(consumer)
            self.await_consumed_messages(consumer)

    def rolling_bounce_brokers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in self.kafka.nodes:
                self.kafka.restart_node(node, clean_shutdown=True)
                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def setup_consumer(self, topic, **kwargs):
        # collect verifiable consumer events since this makes debugging much easier
        consumer = super(OffsetValidationTest, self).setup_consumer(topic, **kwargs)
        self.mark_for_collect(consumer, 'verifiable_consumer_stdout')
        return consumer

    @cluster(num_nodes=7)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_broker_rolling_bounce(self, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify correct consumer behavior when the brokers are consecutively restarted.

        Setup: single Kafka cluster with one producer writing messages to a single topic with one
        partition, a set of consumers in the same group reading from the same topic.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each broker consecutively, waiting for the group to stabilize between
          each broker restart.
        - Verify delivery semantics according to the failure type and that the broker bounces
          did not cause unexpected group rebalances.
        """
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)
        # Due to KIP-899, which rebootstrap is performed when there are no available brokers in the current metadata.
        # We disable rebootstrapping by setting `metadata.recovery.strategy=none` for the consumer, as the test expects no metadata changes.
        # see KAFKA-18194
        consumer = self.setup_consumer(self.TOPIC, group_protocol=group_protocol, prop_file="metadata.recovery.strategy=none")

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)
        self.await_all_members_stabilized(self.TOPIC, self.NUM_PARTITIONS, consumer, timeout_sec=60)

        num_rebalances = consumer.num_rebalances()
        # TODO: make this test work with hard shutdowns, which probably requires
        #       pausing before the node is restarted to ensure that any ephemeral
        #       nodes have time to expire
        self.rolling_bounce_brokers(consumer, clean_shutdown=True)

        unexpected_rebalances = consumer.num_rebalances() - num_rebalances
        assert unexpected_rebalances == 0, \
            "Broker rolling bounce caused %d unexpected group rebalances" % unexpected_rebalances

        consumer.stop_all()

        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

    @cluster(num_nodes=7)
    @matrix(
        clean_shutdown=[True],
        bounce_mode=["all", "rolling"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        clean_shutdown=[True],
        bounce_mode=["all", "rolling"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_consumer_bounce(self, clean_shutdown, bounce_mode, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify correct consumer behavior when the consumers in the group are consecutively restarted.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each consumer, waiting for each one to rejoin the group before
          restarting the rest.
        - Verify delivery semantics according to the failure type.
        """
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=group_protocol)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        if bounce_mode == "all":
            self.bounce_all_consumers(consumer, clean_shutdown=clean_shutdown)
        else:
            self.rolling_bounce_consumers(consumer, clean_shutdown=clean_shutdown)

        consumer.stop_all()
        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records %d did not match consumed position %d" % \
                (consumer.total_consumed(), consumer.current_position(partition))
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position %d greater than the total number of consumed records %d" % \
                (consumer.current_position(partition), consumer.total_consumed())

    @cluster(num_nodes=7)
    @matrix(
        clean_shutdown=[True],
        static_membership=[True, False],
        bounce_mode=["all", "rolling"],
        num_bounces=[5],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        clean_shutdown=[True],
        static_membership=[True, False],
        bounce_mode=["all", "rolling"],
        num_bounces=[5],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=[consumer_group.classic_group_protocol]
    )
    def test_static_consumer_bounce_with_eager_assignment(self, clean_shutdown, static_membership, bounce_mode, num_bounces, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify correct static consumer behavior when the consumers in the group are restarted. In order to make 
        sure the behavior of static members are different from dynamic ones, we take both static and dynamic
        membership into this test suite. This test is based on the eager assignment strategy, where all dynamic consumers 
        revoke their partitions when a global rebalance takes place (even if they are not being bounced). The test relies
        on that eager behaviour when making sure that there is no global rebalance when static members are bounced.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers as static/dynamic members and wait until they've joined the group.
        - In a loop, restart each consumer except the first member (note: may not be the leader), and expect no rebalance triggered
          during this process if the group is in static membership.
        """
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        consumer = self.setup_consumer(self.TOPIC, static_membership=static_membership, group_protocol=group_protocol, 
                                       assignment_strategy="org.apache.kafka.clients.consumer.RangeAssignor")

        consumer.start()
        self.await_all_members(consumer)

        num_revokes_before_bounce = consumer.num_revokes_for_alive()

        num_keep_alive = 1

        if bounce_mode == "all":
            self.bounce_all_consumers(consumer, keep_alive=num_keep_alive, num_bounces=num_bounces)
        else:
            self.rolling_bounce_consumers(consumer, keep_alive=num_keep_alive, num_bounces=num_bounces)

        num_revokes_after_bounce = consumer.num_revokes_for_alive() - num_revokes_before_bounce
            
        # under static membership, the live consumer shall not revoke any current running partitions,
        # since there is no global rebalance being triggered.
        if static_membership:
            assert num_revokes_after_bounce == 0, \
                "Unexpected revocation triggered when bouncing static member. Expecting 0 but had %d revocations" % num_revokes_after_bounce
        else:
            assert num_revokes_after_bounce != 0, \
                "Revocations not triggered as expected when bouncing member with eager assignment"

        consumer.stop_all()
        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records %d did not match consumed position %d" % \
                (consumer.total_consumed(), consumer.current_position(partition))
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position %d greater than the total number of consumed records %d" % \
                (consumer.current_position(partition), consumer.total_consumed())

    @cluster(num_nodes=7)
    @matrix(
        bounce_mode=["all", "rolling"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        bounce_mode=["all", "rolling"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_static_consumer_persisted_after_rejoin(self, bounce_mode, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify that the updated member.id(updated_member_id) caused by static member rejoin would be persisted. If not,
        after the brokers rolling bounce, the migrated group coordinator would load the stale persisted member.id and
        fence subsequent static member rejoin with updated_member_id.

        - Start a producer which continues producing new messages throughout the test.
        - Start up a static consumer and wait until it's up
        - Restart the consumer and wait until it up, its member.id is supposed to be updated and persisted.
        - Rolling bounce all the brokers and verify that the static consumer can still join the group and consumer messages.
        """
        producer = self.setup_producer(self.TOPIC)
        producer.start()
        self.await_produced_messages(producer)
        consumer = self.setup_consumer(self.TOPIC, static_membership=True, group_protocol=group_protocol)
        consumer.start()
        self.await_all_members(consumer)

        # bounce the static member to trigger its member.id updated
        if bounce_mode == "all":
            self.bounce_all_consumers(consumer, num_bounces=1)
        else:
            self.rolling_bounce_consumers(consumer, num_bounces=1)

        # rolling bounce all the brokers to trigger the group coordinator migration and verify updated member.id is persisted
        # and reloaded successfully
        self.rolling_bounce_brokers(consumer, num_bounces=1)

    @cluster(num_nodes=10)
    @matrix(
        num_conflict_consumers=[1, 2],
        fencing_stage=["stable", "all"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        num_conflict_consumers=[1, 2],
        fencing_stage=["stable", "all"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_fencing_static_consumer(self, num_conflict_consumers, fencing_stage, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verify correct static consumer behavior when there are conflicting consumers with same group.instance.id.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers as static members and wait until they've joined the group. Some conflict consumers will be configured with
        - the same group.instance.id.
        - Let normal consumers and fencing consumers start at the same time, and expect only unique consumers left.
        """
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        consumer = self.setup_consumer(self.TOPIC, static_membership=True, group_protocol=group_protocol)

        self.num_consumers = num_conflict_consumers
        conflict_consumer = self.setup_consumer(self.TOPIC, static_membership=True, group_protocol=group_protocol)

        # wait original set of consumer to stable stage before starting conflict members.
        if fencing_stage == "stable":
            consumer.start()
            self.await_members(consumer, len(consumer.nodes))
            self.await_all_members_stabilized(self.TOPIC, self.NUM_PARTITIONS, consumer, timeout_sec=120)

            num_rebalances = consumer.num_rebalances()
            conflict_consumer.start()
            if group_protocol == consumer_group.classic_group_protocol:
                # Classic protocol: conflicting members should join, and the intial ones with conflicting instance id should fail.
                self.await_members(conflict_consumer, num_conflict_consumers)
                self.await_members(consumer, len(consumer.nodes) - num_conflict_consumers)

                wait_until(lambda: len(consumer.dead_nodes()) == num_conflict_consumers,
                       timeout_sec=10,
                       err_msg="Timed out waiting for the fenced consumers to stop")
            else:
                # Consumer protocol: Existing members should remain active and new conflicting ones should not be able to join.
                self.await_consumed_messages(consumer)
                assert num_rebalances == consumer.num_rebalances(), "Static consumers attempt to join with instance id in use should not cause a rebalance"
                assert len(consumer.joined_nodes()) == len(consumer.nodes)
                assert len(conflict_consumer.joined_nodes()) == 0
                
                # Stop existing nodes, so conflicting ones should be able to join.
                consumer.stop_all()
                wait_until(lambda: len(consumer.dead_nodes()) == len(consumer.nodes),
                           timeout_sec=60,
                           err_msg="Timed out waiting for the consumer to shutdown")
                conflict_consumer.start()
                self.await_members(conflict_consumer, num_conflict_consumers)

            
        else:
            consumer.start()
            conflict_consumer.start()

            wait_until(lambda: len(consumer.joined_nodes()) + len(conflict_consumer.joined_nodes()) == len(consumer.nodes),
                       timeout_sec=60,
                       err_msg="Timed out waiting for consumers to join, expected total %d joined, but only see %d joined from "
                               "normal consumer group and %d from conflict consumer group" % \
                               (len(consumer.nodes), len(consumer.joined_nodes()), len(conflict_consumer.joined_nodes()))
                       )
            wait_until(lambda: len(consumer.dead_nodes()) + len(conflict_consumer.dead_nodes()) == len(conflict_consumer.nodes),
                       timeout_sec=60,
                       err_msg="Timed out waiting for fenced consumers to die, expected total %d dead, but only see %d dead in "
                               "normal consumer group and %d dead in conflict consumer group" % \
                               (len(conflict_consumer.nodes), len(consumer.dead_nodes()), len(conflict_consumer.dead_nodes()))
                       )

    @cluster(num_nodes=7)
    @matrix(
        clean_shutdown=[True],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        clean_shutdown=[True],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_consumer_failure(self, clean_shutdown, enable_autocommit, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        partition = TopicPartition(self.TOPIC, 0)

        consumer = self.setup_consumer(self.TOPIC, enable_autocommit=enable_autocommit, group_protocol=group_protocol)
        producer = self.setup_producer(self.TOPIC)

        consumer.start()
        self.await_all_members(consumer)
        self.await_all_members_stabilized(self.TOPIC, self.NUM_PARTITIONS, consumer, timeout_sec=60)
        partition_owner = consumer.owner(partition)

        # startup the producer and ensure that some records have been written
        producer.start()
        self.await_produced_messages(producer)

        # stop the partition owner and await its shutdown
        consumer.kill_node(partition_owner, clean_shutdown=clean_shutdown)
        wait_until(lambda: len(consumer.joined_nodes()) == (self.num_consumers - 1) and consumer.owner(partition) is not None,
                   timeout_sec=60,
                   err_msg="Timed out waiting for consumer to close")

        # ensure that the remaining consumer does some work after rebalancing
        self.await_consumed_messages(consumer, min_messages=1000)

        consumer.stop_all()

        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records %d did not match consumed position %d" % \
                (consumer.total_consumed(), consumer.current_position(partition))
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position %d greater than the total number of consumed records %d" % \
                (consumer.current_position(partition), consumer.total_consumed())

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset %d did not match last consumed position %d" % \
                (consumer.last_commit(partition), consumer.current_position(partition))

    @cluster(num_nodes=7)
    @matrix(
        clean_shutdown=[True, False],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        clean_shutdown=[True, False],
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_broker_failure(self, clean_shutdown, enable_autocommit, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        partition = TopicPartition(self.TOPIC, 0)

        consumer = self.setup_consumer(self.TOPIC, enable_autocommit=enable_autocommit, group_protocol=group_protocol)
        producer = self.setup_producer(self.TOPIC)

        producer.start()
        consumer.start()
        self.await_all_members(consumer)
        self.await_all_members_stabilized(self.TOPIC, self.NUM_PARTITIONS, consumer, timeout_sec=60)

        num_rebalances = consumer.num_rebalances()

        # shutdown one of the brokers
        # TODO: we need a way to target the coordinator instead of picking arbitrarily
        self.kafka.signal_node(self.kafka.nodes[0], signal.SIGTERM if clean_shutdown else signal.SIGKILL)

        # ensure that the consumers do some work after the broker failure
        self.await_consumed_messages(consumer, min_messages=1000)

        # verify that there were no rebalances on failover.
        assert num_rebalances == consumer.num_rebalances(), "Broker failure should not cause a rebalance"

        consumer.stop_all()

        # if the total records consumed matches the current position, we haven't seen any duplicates
        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset %d did not match last consumed position %d" % \
                (consumer.last_commit(partition), consumer.current_position(partition))

    @cluster(num_nodes=7)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_group_consumption(self, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None):
        """
        Verifies correct group rebalance behavior as consumers are started and stopped.
        In particular, this test verifies that the partition is readable after every
        expected rebalance.

        Setup: single Kafka cluster with a group of consumers reading from one topic
        with one partition while the verifiable producer writes to it.

        - Start the consumers one by one, verifying consumption after each rebalance
        - Shutdown the consumers one by one, verifying consumption after each rebalance
        """
        consumer = self.setup_consumer(self.TOPIC, group_protocol=group_protocol)
        producer = self.setup_producer(self.TOPIC)

        partition = TopicPartition(self.TOPIC, 0)

        producer.start()

        for num_started, node in enumerate(consumer.nodes, 1):
            consumer.start_node(node)
            self.await_members(consumer, num_started)
            self.await_consumed_messages(consumer)

        for num_stopped, node in enumerate(consumer.nodes, 1):
            consumer.stop_node(node)

            if num_stopped < self.num_consumers:
                self.await_members(consumer, self.num_consumers - num_stopped)
                self.await_consumed_messages(consumer)

        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

        assert consumer.last_commit(partition) == consumer.current_position(partition), \
            "Last committed offset %d did not match last consumed position %d" % \
            (consumer.last_commit(partition), consumer.current_position(partition))

class AssignmentValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 6

    def __init__(self, test_context):
        super(AssignmentValidationTest, self).__init__(test_context, num_consumers=3, num_producers=0,
                                                num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 },
        })

    @cluster(num_nodes=6)
    @matrix(
        assignment_strategy=["org.apache.kafka.clients.consumer.RangeAssignor",
                             "org.apache.kafka.clients.consumer.RoundRobinAssignor",
                             "org.apache.kafka.clients.consumer.StickyAssignor",
                             "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[False]
    )
    @matrix(
        assignment_strategy=["org.apache.kafka.clients.consumer.RangeAssignor",
                             "org.apache.kafka.clients.consumer.RoundRobinAssignor",
                             "org.apache.kafka.clients.consumer.StickyAssignor",
                             "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=[consumer_group.classic_group_protocol],
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        group_protocol=[consumer_group.consumer_group_protocol],
        group_remote_assignor=consumer_group.all_remote_assignors
    )
    def test_valid_assignment(self, assignment_strategy=None, metadata_quorum=quorum.zk, use_new_coordinator=False, group_protocol=None, group_remote_assignor=None):
        """
        Verify assignment strategy correctness: each partition is assigned to exactly
        one consumer instance.

        Setup: single Kafka cluster with a set of consumers in the same group.

        - Start the consumers one by one
        - Validate assignment after every expected rebalance
        """
        consumer = self.setup_consumer(self.TOPIC,
                                       assignment_strategy=assignment_strategy,
                                       group_protocol=group_protocol,
                                       group_remote_assignor=group_remote_assignor)
        for num_started, node in enumerate(consumer.nodes, 1):
            consumer.start_node(node)
            self.await_members(consumer, num_started)
            wait_until(lambda: self.valid_assignment(self.TOPIC, self.NUM_PARTITIONS, consumer.current_assignment()),
                timeout_sec=30,
                err_msg="expected valid assignments of %d partitions when num_started %d: %s" % \
                        (self.NUM_PARTITIONS, num_started, \
                         [(str(node.account), a) for node, a in consumer.current_assignment().items()]))
