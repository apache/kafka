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
from kafkatest.version import LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, \
    LATEST_1_0, LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, \
    LATEST_2_7, LATEST_2_8, LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, \
    LATEST_3_7, LATEST_3_8, DEV_BRANCH, KafkaVersion

class ConsumerProtocolMigrationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 6

    RANGE = "org.apache.kafka.clients.consumer.RangeAssignor"
    ROUND_ROBIN = "org.apache.kafka.clients.consumer.RoundRobinAssignor"
    STICKY = "org.apache.kafka.clients.consumer.StickyAssignor"
    COOPERATIVE_STICKEY = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    all_assignment_strategies = [RANGE, ROUND_ROBIN, COOPERATIVE_STICKEY, STICKY]

    all_consumer_versions = [LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, \
                             LATEST_1_0, LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, \
                             LATEST_2_7, LATEST_2_8, LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, \
                             LATEST_3_7, LATEST_3_8, DEV_BRANCH]
    consumer_versions_supporting_sticky_assignor = [v for v in all_consumer_versions if v >= LATEST_0_11_0]
    consumer_versions_supporting_cooperative_sticky_assignor = [v for v in all_consumer_versions if v >= LATEST_2_4]
    consumer_versions_supporting_static_membership = [v for v in all_consumer_versions if v >= LATEST_2_3]

    def __init__(self, test_context):
        super(ConsumerProtocolMigrationTest, self).__init__(test_context, num_consumers=5, num_producers=1,
                                                            num_zk=0, num_brokers=1,
                                                            use_new_coordinator=True, topics={
                self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 }
            })

    def bounce_all_consumers(self, consumer, clean_shutdown=True):
        for node in consumer.nodes:
            consumer.stop_node(node, clean_shutdown)

        wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers, timeout_sec=10,
                   err_msg="Timed out waiting for the consumers to shutdown")

        # Wait until the group becomes empty. We use the 50-second timeout because the
        # consumer session timeout is 45 seconds.
        wait_until(lambda: self.group_id in self.kafka.list_consumer_groups(state="empty"),
                   timeout_sec=50,
                   err_msg="Timed out waiting for the group to become empty.")

        for node in consumer.nodes:
            consumer.start_node(node)

        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)

    def rolling_bounce_consumers(self, consumer, clean_shutdown=True):
        for node in consumer.nodes:
            consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == 1,
                       timeout_sec=self.session_timeout_sec+5,
                       err_msg="Timed out waiting for the consumer to shutdown")

            consumer.start_node(node)

            self.await_all_members(consumer)
            self.await_consumed_messages(consumer)

    def set_group_instance_id(self, consumer):
        consumer.static_membership = True
        for ind, node in enumerate(consumer.nodes):
            node.group_instance_id = "migration-test-member-%d" % ind

    def set_consumer_version(self, consumer, version):
        for node in consumer.nodes:
            node.version = version

    def assert_group_type(self, type, timeout_sec=10):
        wait_until(lambda: self.group_id in self.kafka.list_consumer_groups(type=type), timeout_sec=timeout_sec,
                   err_msg="Timed out waiting to list expected %s group." % type)

    @cluster(num_nodes=8)
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional", "upgrade", "downgrade", "disabled"],
        consumer_version=[str(v) for v in all_consumer_versions],
        assignment_strategy=[RANGE, ROUND_ROBIN]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional", "upgrade", "downgrade", "disabled"],
        consumer_version=[str(v) for v in consumer_versions_supporting_sticky_assignor],
        assignment_strategy=[STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional", "upgrade", "downgrade", "disabled"],
        consumer_version=[str(v) for v in consumer_versions_supporting_static_membership],
        assignment_strategy=[RANGE, ROUND_ROBIN, STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional", "upgrade", "downgrade", "disabled"],
        consumer_version=[str(v) for v in consumer_versions_supporting_cooperative_sticky_assignor],
        assignment_strategy=[COOPERATIVE_STICKEY]
    )
    def test_consumer_offline_migration(self, enable_autocommit, static_membership, metadata_quorum,
                                        consumer_group_migration_policy, consumer_version, assignment_strategy):
        """
        Verify correct consumer behavior when the consumers in the group are restarted to perform
        offline upgrade/downgrade.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers with classic protocols and wait until they've joined the group.
        - Offline upgrade: restart the consumers with consumer protocols, waiting for all consumers
          to finish shutting down before starting up them.
        - Offline downgrade: Restart the consumers with classic protocols, waiting for all consumers
          to finish shutting down before starting up them.
        - Verify delivery semantics according to the failure type.
        """
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.classic_group_protocol,
                                       version=consumer_version, assignment_strategy=assignment_strategy,
                                       enable_autocommit=enable_autocommit)

        kafka_version = KafkaVersion(consumer_version)
        if kafka_version == LATEST_2_3 or kafka_version == LATEST_2_4 or (static_membership and kafka_version > LATEST_2_4):
            # group-instance-id is required in 2.3.
            self.set_group_instance_id(consumer)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)
        self.assert_group_type("classic")

        # Upgrade the group protocol and restart all consumers.
        consumer.group_protocol = consumer_group.consumer_group_protocol
        self.set_consumer_version(consumer, DEV_BRANCH)

        self.bounce_all_consumers(consumer)
        self.assert_group_type("consumer")

        # Downgrade the group protocol and restart all consumers.
        consumer.group_protocol = consumer_group.classic_group_protocol
        self.set_consumer_version(consumer, kafka_version)
        self.bounce_all_consumers(consumer)
        self.assert_group_type("classic")

        consumer.stop_all()

    @cluster(num_nodes=8)
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional"],
        consumer_version=[str(v) for v in all_consumer_versions],
        assignment_strategy=[RANGE, ROUND_ROBIN]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional"],
        consumer_version=[str(v) for v in consumer_versions_supporting_sticky_assignor],
        assignment_strategy=[STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional"],
        consumer_version=[str(v) for v in consumer_versions_supporting_static_membership],
        assignment_strategy=[RANGE, ROUND_ROBIN, STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["bidirectional"],
        consumer_version=[str(v) for v in consumer_versions_supporting_cooperative_sticky_assignor],
        assignment_strategy=[COOPERATIVE_STICKEY]
    )
    def test_consumer_rolling_migration(self, enable_autocommit, static_membership, metadata_quorum,
                                        consumer_group_migration_policy, consumer_version, assignment_strategy):
        """
        Verify correct consumer behavior when the consumers in the group are restarted to perform
        online upgrade/downgrade when the migration policy is set to be BIDIRECTIONAL.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers with classic protocols and wait until they've joined the group.
        - Online upgrade: In a loop, restart each consumer with consumer protocol, waiting for
          each one to rejoin the group before restarting the rest.
        - Online downgrade: In a loop, restart each consumer with classic protocol, waiting for
          each one to rejoin the group before restarting the rest.
        - Verify delivery semantics according to the failure type.
        """
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.classic_group_protocol,
                                       version=consumer_version, assignment_strategy=assignment_strategy,
                                       enable_autocommit=enable_autocommit)

        kafka_version = KafkaVersion(consumer_version)
        if kafka_version == LATEST_2_3 or kafka_version == LATEST_2_4 or (static_membership and kafka_version > LATEST_2_4):
            # group-instance-id is required in 2.3.
            self.set_group_instance_id(consumer)

        producer.start()
        self.await_produced_messages(producer)

        self.set_consumer_version(consumer, kafka_version)
        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)
        self.assert_group_type("classic")

        # Upgrade the group protocol and rolling restart the consumers.
        consumer.group_protocol = consumer_group.consumer_group_protocol
        self.set_consumer_version(consumer, DEV_BRANCH)
        self.rolling_bounce_consumers(consumer)
        self.assert_group_type("consumer")

        # Downgrade the group protocol and rolling restart the consumers.
        consumer.group_protocol = consumer_group.classic_group_protocol
        self.set_consumer_version(consumer, kafka_version)
        self.rolling_bounce_consumers(consumer)
        self.assert_group_type("classic", consumer.session_timeout_sec+5)

        consumer.stop_all()

    @cluster(num_nodes=8)
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["upgrade"],
        consumer_version=[str(v) for v in all_consumer_versions],
        assignment_strategy=[RANGE, ROUND_ROBIN]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["upgrade"],
        consumer_version=[str(v) for v in consumer_versions_supporting_sticky_assignor],
        assignment_strategy=[STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["upgrade"],
        consumer_version=[str(v) for v in consumer_versions_supporting_static_membership],
        assignment_strategy=[RANGE, ROUND_ROBIN, STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["upgrade"],
        consumer_version=[str(v) for v in consumer_versions_supporting_cooperative_sticky_assignor],
        assignment_strategy=[COOPERATIVE_STICKEY]
    )
    def test_consumer_rolling_upgrade(self, enable_autocommit, static_membership, metadata_quorum,
                                      consumer_group_migration_policy, consumer_version, assignment_strategy):
        """
        Verify correct consumer behavior when the consumers in the group are restarted to perform
        online upgrade when the migration policy is set to be UPGRADE.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers with classic protocols and wait until they've joined the group.
        - Online upgrade: In a loop, restart each consumer with consumer protocol, waiting for
          each one to rejoin the group before restarting the rest.
        - Verify delivery semantics according to the failure type.
        """
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.classic_group_protocol,
                                       version=consumer_version, assignment_strategy=assignment_strategy,
                                       enable_autocommit=enable_autocommit)

        kafka_version = KafkaVersion(consumer_version)
        if kafka_version == LATEST_2_3 or kafka_version == LATEST_2_4 or (static_membership and kafka_version > LATEST_2_4):
            # group-instance-id is required in 2.3.
            self.set_group_instance_id(consumer)

        producer.start()
        self.await_produced_messages(producer)

        self.set_consumer_version(consumer, kafka_version)
        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)
        self.assert_group_type("classic")

        # Upgrade the group protocol and rolling restart the consumers.
        consumer.group_protocol = consumer_group.consumer_group_protocol
        self.set_consumer_version(consumer, DEV_BRANCH)
        self.rolling_bounce_consumers(consumer)
        self.assert_group_type("consumer")

        consumer.stop_all()

    @cluster(num_nodes=8)
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["downgrade"],
        consumer_version=[str(v) for v in all_consumer_versions],
        assignment_strategy=[RANGE, ROUND_ROBIN]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["downgrade"],
        consumer_version=[str(v) for v in consumer_versions_supporting_sticky_assignor],
        assignment_strategy=[STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["downgrade"],
        consumer_version=[str(v) for v in consumer_versions_supporting_static_membership],
        assignment_strategy=[RANGE, ROUND_ROBIN, STICKY]
    )
    @matrix(
        enable_autocommit=[True, False],
        static_membership=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        consumer_group_migration_policy=["downgrade"],
        consumer_version=[str(v) for v in consumer_versions_supporting_cooperative_sticky_assignor],
        assignment_strategy=[COOPERATIVE_STICKEY]
    )
    def test_consumer_rolling_downgrade(self, enable_autocommit, static_membership, metadata_quorum,
                                        consumer_group_migration_policy, consumer_version, assignment_strategy):
        """
        Verify correct consumer behavior when the consumers in the group are restarted to perform
        online downgrade when the migration policy is set to be DOWNGRADE.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers with consumer protocols and wait until they've joined the group.
        - Online downgrade: In a loop, restart each consumer with classic protocol, waiting for
          each one to rejoin the group before restarting the rest.
        - Verify delivery semantics according to the failure type.
        """
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.consumer_group_protocol,
                                       assignment_strategy=assignment_strategy,
                                       enable_autocommit=enable_autocommit)

        kafka_version = KafkaVersion(consumer_version)
        if kafka_version == LATEST_2_3 or kafka_version == LATEST_2_4 or (static_membership and kafka_version > LATEST_2_4):
            # group-instance-id is required in 2.3.
            self.set_group_instance_id(consumer)

        producer.start()
        self.await_produced_messages(producer)

        self.set_consumer_version(consumer, DEV_BRANCH)
        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)
        self.assert_group_type("consumer")

        # Downgrade the group protocol and rolling restart the consumers.
        consumer.group_protocol = consumer_group.classic_group_protocol
        self.set_consumer_version(consumer, kafka_version)
        self.rolling_bounce_consumers(consumer)
        self.assert_group_type("classic")

        consumer.stop_all()
