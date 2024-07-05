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
                             LATEST_1_0, LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_4, LATEST_2_5, LATEST_2_6, \
                             LATEST_2_7, LATEST_2_8, LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, \
                             LATEST_3_7, LATEST_3_8, DEV_BRANCH]
    # all_consumer_versions = [LATEST_2_3]

    def __init__(self, test_context):
        super(ConsumerProtocolMigrationTest, self).__init__(test_context, num_consumers=5, num_producers=1,
                                                            num_zk=0, num_brokers=1, topics={
                self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 }
            })

    def bounce_all_consumers(self, consumer, clean_shutdown=True):
        for node in consumer.nodes:
            consumer.stop_node(node, clean_shutdown)

        wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers, timeout_sec=10,
                   err_msg="Timed out waiting for the consumers to shutdown")

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

    @cluster(num_nodes=12)
    @matrix(
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        consumer_group_migration_policy=["bidirectional", "upgrade", "downgrade", "disabled"],
        consumer_version=[str(v) for v in all_consumer_versions],
        assignment_strategy=all_assignment_strategies
    )
    def test_consumer_offline_migration(self, enable_autocommit, metadata_quorum, use_new_coordinator,
                                  consumer_group_migration_policy, consumer_version, assignment_strategy):
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.classic_group_protocol,
                                       version=consumer_version, assignment_strategy=assignment_strategy,
                                       enable_autocommit=enable_autocommit)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)

        # Upgrade the group protocol and restart all consumers.
        consumer.group_protocol = consumer_group.consumer_group_protocol
        self.bounce_all_consumers(consumer)

        # Downgrade the group protocol and restart all consumers.
        # Clean shutdown is required for downgrade as the server needs to be informed of the consumers' leaving.
        consumer.group_protocol = consumer_group.classic_group_protocol
        self.bounce_all_consumers(consumer)

        consumer.stop_all()

    @cluster(num_nodes=12)
    @matrix(
        enable_autocommit=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True],
        consumer_group_migration_policy=["bidirectional"],
        consumer_version=[str(v) for v in all_consumer_versions],
        assignment_strategy=all_assignment_strategies
    )
    def test_consumer_rolling_migration(self, enable_autocommit, metadata_quorum, use_new_coordinator,
                                      consumer_group_migration_policy, consumer_version, assignment_strategy):
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC, group_protocol=consumer_group.classic_group_protocol,
                                       version=consumer_version, assignment_strategy=assignment_strategy,
                                       enable_autocommit=enable_autocommit)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)
        self.await_consumed_messages(consumer)

        # Upgrade the group protocol and rolling restart the consumers.
        consumer.group_protocol = consumer_group.consumer_group_protocol
        self.rolling_bounce_consumers(consumer)

        # Downgrade the group protocol and rolling restart the consumers.
        # Clean shutdown is required for downgrade as the server needs to be informed of the consumers' leaving.
        consumer.group_protocol = consumer_group.classic_group_protocol
        self.rolling_bounce_consumers(consumer)

        consumer.stop_all()
