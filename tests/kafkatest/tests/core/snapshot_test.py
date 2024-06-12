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

from kafkatest.services.kafka import quorum
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import config_property
from kafkatest.services.kafka import consumer_group
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
import random

class TestSnapshots(ProduceConsumeValidateTest):

    TOPIC_NAME_PREFIX = "test_topic_"

    def __init__(self, test_context):
        super(TestSnapshots, self).__init__(test_context=test_context)
        self.topics_created = 0
        self.topic = "test_topic"
        self.partitions = 3
        self.replication_factor = 3
        self.num_nodes = 3

        # Producer and consumer
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

        security_protocol = 'PLAINTEXT'
        # Setup Custom Config to ensure snapshot will be generated deterministically
        self.kafka = KafkaService(self.test_context, self.num_nodes, zk=None,
                                  topics={self.topic: {"partitions": self.partitions,
                                                       "replication-factor": self.replication_factor,
                                                       'configs': {"min.insync.replicas": 2}}},
                                  server_prop_overrides=[
                                      [config_property.METADATA_LOG_DIR, KafkaService.METADATA_LOG_DIR],
                                      [config_property.METADATA_LOG_SEGMENT_MS, "10000"],
                                      [config_property.METADATA_LOG_RETENTION_BYTES, "2048"],
                                      [config_property.METADATA_LOG_BYTES_BETWEEN_SNAPSHOTS, "2048"]
                                  ])

        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.security_protocol = security_protocol

    def setUp(self):
        # Start the cluster and ensure that a snapshot is generated
        self.logger.info("Starting the cluster and running until snapshot creation")

        assert quorum.for_test(self.test_context) in quorum.all_kraft, \
                "Snapshot test should be run Kraft Modes only"

        self.kafka.start()

        topic_count = 10
        self.topics_created += self.create_n_topics(topic_count)

        if self.kafka.isolated_controller_quorum:
            self.controller_nodes = self.kafka.isolated_controller_quorum.nodes
        else:
            self.controller_nodes = self.kafka.nodes[:self.kafka.num_nodes_controller_role]

        # Waiting for snapshot creation and first log segment
        # cleanup on all controller nodes
        for node in self.controller_nodes:
            self.logger.debug("Waiting for snapshot on: %s" % self.kafka.who_am_i(node))
            self.wait_for_log_segment_delete(node)
            self.wait_for_snapshot(node)
        self.logger.debug("Verified Snapshots exist on controller nodes")

    def create_n_topics(self, topic_count):
        for i in range(self.topics_created, topic_count):
            topic = "%s%d" % (TestSnapshots.TOPIC_NAME_PREFIX, i)
            self.logger.debug("Creating topic %s" % topic)
            topic_cfg = {
                "topic": topic,
                "partitions": self.partitions,
                "replication-factor": self.replication_factor,
                "configs": {"min.insync.replicas": 2}
            }
            self.kafka.create_topic(topic_cfg)
        self.logger.debug("Created %d more topics" % topic_count)
        return topic_count

    def wait_for_log_segment_delete(self, node):
        file_path = self.kafka.METADATA_FIRST_LOG
        # Wait until the first log segment in metadata log is marked for deletion
        wait_until(lambda: not self.file_exists(node, file_path),
                   timeout_sec=100,
                   backoff_sec=1, err_msg="Not able to verify cleanup of log file %s in a reasonable amount of time" % file_path)

    def wait_for_snapshot(self, node):
        # Wait for a snapshot file to show up
        file_path = self.kafka.METADATA_SNAPSHOT_SEARCH_STR
        wait_until(lambda: self.file_exists(node, file_path),
                   timeout_sec=100,
                   backoff_sec=1, err_msg="Not able to verify snapshot existence in a reasonable amount of time")

    def file_exists(self, node, file_path):
        # Check if the first log segment is cleaned up
        self.logger.debug("Checking if file %s exists" % file_path)
        cmd = "ls %s" % file_path
        files = node.account.ssh_output(cmd, allow_fail=True, combine_stderr=False)

        if not files:
            self.logger.debug("File %s does not exist" % file_path)
            return False
        else:
            self.logger.debug("File %s was found" % file_path)
            return True

    def validate_success(self, topic = None, group_protocol=None):
        if topic is None:
            # Create a new topic
            topic = "%s%d" % (TestSnapshots.TOPIC_NAME_PREFIX, self.topics_created)
            self.topics_created += self.create_n_topics(topic_count=1)

        # Produce to the newly created topic to ensure broker has caught up
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           topic, throughput=self.producer_throughput,
                                           message_validator=is_int)

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        topic, consumer_timeout_ms=30000,
                                        message_validator=is_int,
                                        consumer_properties=consumer_group.maybe_set_group_protocol(group_protocol))
        self.start_producer_and_consumer()
        self.stop_producer_and_consumer()
        self.validate()

    @cluster(num_nodes=9)
    @matrix(
        metadata_quorum=quorum.all_kraft,
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=quorum.all_kraft,
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_broker(self, metadata_quorum=quorum.combined_kraft, use_new_coordinator=False, group_protocol=None):
        """ Test the ability of a broker to consume metadata snapshots
        and to recover the cluster metadata state using them

        The test ensures that that there is atleast one snapshot created on
        the controller quorum during the setup phase and that at least the first
        log segment in the metadata log has been marked for deletion, thereby ensuring
        that any observer of the log needs to always load a snapshot to catch
        up to the current metadata state.

        Each scenario is a progression over the previous one.
        The scenarios build on top of each other by:
        * Loading a snapshot
        * Loading and snapshot and some delta records
        * Loading a snapshot and delta and ensuring that the most recent metadata state
          has been caught up.

        Even though a subsequent scenario covers the previous one, they are all
        left in the test to make debugging a failure of the test easier
        e.g. if the first scenario passes and the second fails, it hints towards
        a problem with the application of delta records while catching up
        """

        # Scenario -- Re-init broker after cleaning up all persistent state
        node = random.choice(self.kafka.nodes)
        self.logger.debug("Scenario: kill-clean-start on broker node %s", self.kafka.who_am_i(node))
        self.kafka.clean_node(node)
        self.kafka.start_node(node)

        # Scenario -- Re-init broker after cleaning up all persistent state
        # Create some metadata changes for the broker to consume as well.
        node = random.choice(self.kafka.nodes)
        self.logger.debug("Scenario: kill-clean-create_topics-start on broker node %s", self.kafka.who_am_i(node))
        self.kafka.clean_node(node)
        # Now modify the cluster to create more metadata changes
        self.topics_created += self.create_n_topics(topic_count=10)
        self.kafka.start_node(node)

        # Scenario -- Re-init broker after cleaning up all persistent state
        # And ensure that the broker has replicated the metadata log
        node = random.choice(self.kafka.nodes)
        self.logger.debug("Scenario: kill-clean-start-verify-produce on broker node %s", self.kafka.who_am_i(node))
        self.kafka.clean_node(node)
        self.kafka.start_node(node)
        # Create a topic where the affected broker must be the leader
        broker_topic = "%s%d" % (TestSnapshots.TOPIC_NAME_PREFIX, self.topics_created)
        self.topics_created += 1
        self.logger.debug("Creating topic %s" % broker_topic)
        topic_cfg = {
            "topic": broker_topic,
            "replica-assignment": self.kafka.idx(node),
            "configs": {"min.insync.replicas": 1}
        }
        self.kafka.create_topic(topic_cfg)

        # Produce to the newly created topic and make sure it works.
        self.validate_success(broker_topic, group_protocol=group_protocol)

    @cluster(num_nodes=9)
    @matrix(
        metadata_quorum=quorum.all_kraft,
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=quorum.all_kraft,
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_controller(self, metadata_quorum=quorum.combined_kraft, use_new_coordinator=False, group_protocol=None):
        """ Test the ability of controllers to consume metadata snapshots
        and to recover the cluster metadata state using them

        The test ensures that that there is atleast one snapshot created on
        the controller quorum during the setup phase and that at least the first
        log segment in the metadata log has been marked for deletion, thereby ensuring
        that any observer of the log needs to always load a snapshot to catch
        up to the current metadata state.

        Each scenario is a progression over the previous one.
        The scenarios build on top of each other by:
        * Loading a snapshot
        * Loading and snapshot and some delta records
        * Loading a snapshot and delta and ensuring that the most recent metadata state
          has been caught up.

        Even though a subsequent scenario covers the previous one, they are all
        left in the test to make debugging a failure of the test easier
        e.g. if the first scenario passes and the second fails, it hints towards
        a problem with the application of delta records while catching up
        """

        # Scenario -- Re-init controllers with a clean kafka dir
        self.logger.debug("Scenario: kill-clean-start controller node")
        for node in self.controller_nodes:
            self.logger.debug("Restarting node: %s", self.kafka.controller_quorum.who_am_i(node))
            self.kafka.controller_quorum.clean_node(node)
            self.kafka.controller_quorum.start_node(node)

        # Scenario -- Re-init controllers with a clean kafka dir and
        # make metadata changes while they are down.
        # This will force the entire quorum to load from snapshots
        # and verify the quorum's ability to catch up to the latest metadata
        self.logger.debug("Scenario: kill-clean-create_topics-start on controller node %s")
        for node in self.controller_nodes:
            self.logger.debug("Restarting node: %s", self.kafka.controller_quorum.who_am_i(node))
            self.kafka.controller_quorum.clean_node(node)
            # Now modify the cluster to create more metadata changes
            self.topics_created += self.create_n_topics(topic_count=5)
            self.kafka.controller_quorum.start_node(node)

        # Produce to a newly created topic and make sure it works.
        self.validate_success(group_protocol=group_protocol)
