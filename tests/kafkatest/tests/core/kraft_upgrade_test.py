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

from ducktape.mark import parametrize, matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.quorum import isolated_kraft, combined_kraft
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, \
    LATEST_3_6, LATEST_3_7, LATEST_3_8, LATEST_3_9, DEV_BRANCH, KafkaVersion, LATEST_STABLE_METADATA_VERSION

#
# Test upgrading between different KRaft versions.
#
# Note that the earliest supported KRaft version is 3.0, not 0.8 as it is for
# ZK mode. The upgrade process is also somewhat different for KRaft because we
# use metadata.version instead of inter.broker.protocol.
#
class TestKRaftUpgrade(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(TestKRaftUpgrade, self).__init__(test_context=test_context)
        self.may_truncate_acked_records = False

    def setUp(self):
        self.topic = "test_topic"
        self.partitions = 3
        self.replication_factor = 3

        # Producer and consumer
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def wait_until_rejoin(self):
        for partition in range(0, self.partitions):
            wait_until(lambda: len(self.kafka.isr_idx_list(self.topic, partition)) == self.replication_factor, timeout_sec=60,
                    backoff_sec=1, err_msg="Replicas did not rejoin the ISR in a reasonable amount of time")

    def upgrade_to_dev_version(self, from_kafka_version, update_metadata_version):
        self.logger.info("Performing rolling upgrade.")
        for node in self.kafka.controller_quorum.nodes:
            self.logger.info("Stopping controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.stop_node(node)
            node.version = DEV_BRANCH
            self.logger.info("Restarting controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted controller node %s" % node.account.hostname)
        for node in self.kafka.nodes:
            self.logger.info("Stopping broker node %s" % node.account.hostname)
            self.kafka.stop_node(node)
            node.version = DEV_BRANCH
            self.logger.info("Restarting broker node %s" % node.account.hostname)
            self.kafka.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted broker node %s" % node.account.hostname)
        if update_metadata_version:
            self.logger.info("Changing metadata.version to %s" % LATEST_STABLE_METADATA_VERSION)
            self.kafka.upgrade_metadata_version(LATEST_STABLE_METADATA_VERSION)

    def downgrade_to_version(self, to_kafka_version):
        self.logger.info("Performing rolling downgrade.")
        for node in self.kafka.controller_quorum.nodes:
            self.logger.info("Stopping controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.stop_node(node)
            node.version = KafkaVersion(to_kafka_version)
            self.logger.info("Restarting controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted controller node %s" % node.account.hostname)
        for node in self.kafka.nodes:
            self.logger.info("Stopping broker node %s" % node.account.hostname)
            self.kafka.stop_node(node)
            node.version = KafkaVersion(to_kafka_version)
            self.logger.info("Restarting broker node %s" % node.account.hostname)
            self.kafka.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted broker node %s" % node.account.hostname)

    def run_upgrade(self, from_kafka_version):
        """Test upgrade of Kafka broker cluster from various versions to the current version

        from_kafka_version is a Kafka version to upgrade from.

        - Start 3 node broker cluster on version 'from_kafka_version'.
        - Start producer and consumer in the background.
        - Perform rolling upgrade.
        - Upgrade cluster to the latest metadata.version.
        - Finally, validate that every message acked by the producer was consumed by the consumer.
        """
        fromKafkaVersion = KafkaVersion(from_kafka_version)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=None,
                                  version=fromKafkaVersion,
                                  topics={self.topic: {"partitions": self.partitions,
                                                       "replication-factor": self.replication_factor,
                                                       'configs': {"min.insync.replicas": 2}}})
        self.kafka.start()
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=["none"],
                                           version=KafkaVersion(from_kafka_version))
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=KafkaVersion(from_kafka_version))

        self.run_produce_consume_validate(core_test_action=lambda: self.upgrade_to_dev_version(from_kafka_version, True))
        cluster_id = self.kafka.cluster_id()
        assert cluster_id is not None
        assert len(cluster_id) == 22
        assert self.kafka.check_protocol_errors(self)

    def run_upgrade_downgrade(self, starting_kafka_version):
        """Test upgrade and downgrade of Kafka broker cluster from various versions to current version and back

        - Start 3 node broker cluster on version 'starting_kafka_version'.
        - Perform rolling upgrade but do not update metadata.version.
        - Start producer and consumer in the background.
        - Perform rolling downgrade.
        - Finally, validate that every message acked by the producer was consumed by the consumer.
        """
        fromKafkaVersion = KafkaVersion(starting_kafka_version)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=None,
                                  version=fromKafkaVersion,
                                  topics={self.topic: {"partitions": self.partitions,
                                                       "replication-factor": self.replication_factor,
                                                       'configs': {"min.insync.replicas": 2}}})
        self.kafka.start()
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=["none"],
                                           version=KafkaVersion(starting_kafka_version))
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=KafkaVersion(starting_kafka_version))
        self.upgrade_to_dev_version(starting_kafka_version, False)

        self.run_produce_consume_validate(core_test_action=lambda: self.downgrade_to_version(starting_kafka_version))
        cluster_id = self.kafka.cluster_id()
        assert cluster_id is not None
        assert len(cluster_id) == 22
        assert self.kafka.check_protocol_errors(self)

    @cluster(num_nodes=5)
    @matrix(from_kafka_version=[str(LATEST_3_1), str(LATEST_3_2), str(LATEST_3_3), str(LATEST_3_4), str(LATEST_3_5), str(LATEST_3_6), str(LATEST_3_7), str(LATEST_3_8), str(LATEST_3_9), str(DEV_BRANCH)],
            metadata_quorum=[combined_kraft])
    def test_combined_mode_upgrade(self, from_kafka_version, metadata_quorum, use_new_coordinator=False):
        self.run_upgrade(from_kafka_version)

    @cluster(num_nodes=8)
    @matrix(from_kafka_version=[str(LATEST_3_1), str(LATEST_3_2), str(LATEST_3_3), str(LATEST_3_4), str(LATEST_3_5), str(LATEST_3_6), str(LATEST_3_7), str(LATEST_3_8), str(LATEST_3_9), str(DEV_BRANCH)],
            metadata_quorum=[isolated_kraft])
    def test_isolated_mode_upgrade(self, from_kafka_version, metadata_quorum, use_new_coordinator=False):
        self.run_upgrade(from_kafka_version)

    @cluster(num_nodes=5)
    @matrix(from_kafka_version=[str(LATEST_3_3), str(LATEST_3_4), str(LATEST_3_5), str(LATEST_3_6), str(LATEST_3_7), str(LATEST_3_8), str(LATEST_3_9), str(DEV_BRANCH)],
            metadata_quorum=[combined_kraft])
    def test_combined_mode_upgrade_downgrade(self, from_kafka_version, metadata_quorum, use_new_coordinator=False):
        self.run_upgrade_downgrade(from_kafka_version)

    @cluster(num_nodes=8)
    @matrix(from_kafka_version=[str(LATEST_3_3), str(LATEST_3_4), str(LATEST_3_5), str(LATEST_3_6), str(LATEST_3_7), str(LATEST_3_8), str(LATEST_3_9), str(DEV_BRANCH)],
            metadata_quorum=[isolated_kraft])
    def test_isolated_mode_upgrade_downgrade(self, from_kafka_version, metadata_quorum, use_new_coordinator=False):
        self.run_upgrade_downgrade(from_kafka_version)