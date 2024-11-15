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

import json
import re

from functools import partial
from typing import List

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.quorum import combined_kraft, ServiceQuorumInfo, isolated_kraft
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import DEV_BRANCH

#
# Test quorum reconfiguration for combined and isolated mode
#
class TestQuorumReconfiguration(ProduceConsumeValidateTest):
    def __init__(self, test_context: TestContext):
        super(TestQuorumReconfiguration, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.partitions = 3
        self.replication_factor = 3

        # Producer and consumer
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def perform_reconfig(self,
                         active_controller_id: int,
                         inactive_controller_id: int,
                         inactive_controller: ClusterNode,
                         broker_only_ids: List[int]):
        """
        Tests quorum reconfiguration by adding a second controller and then removing the active controller.

        :param active_controller_id: id of the active controller
        :param inactive_controller_id: id of the inactive controller
        :param inactive_controller: node object of the inactive controller
        :param broker_only_ids: broker ids of nodes which have no controller process
        """
        # Check describe quorum output shows the controller (first node) is the leader and the only voter
        wait_until(lambda: check_describe_quorum_output(self.kafka.describe_quorum(),
                                                        active_controller_id,
                                                        [active_controller_id],
                                                        broker_only_ids), timeout_sec=5)
        # Start second controller
        self.kafka.controller_quorum.add_broker(inactive_controller)
        wait_until(lambda: check_describe_quorum_output(self.kafka.describe_quorum(),
                                                        active_controller_id,
                                                        [active_controller_id],
                                                        broker_only_ids + [inactive_controller_id]), timeout_sec=5)
        # Add controller to quorum
        self.kafka.controller_quorum.add_controller(inactive_controller_id, inactive_controller)

        # Check describe quorum output shows both controllers are voters
        wait_until(lambda: check_describe_quorum_output(self.kafka.describe_quorum(),
                                                        active_controller_id,
                                                        [active_controller_id, inactive_controller_id],
                                                        broker_only_ids), timeout_sec=5)
        # Remove leader from quorum
        voters = json_from_line(r"CurrentVoters:.*", self.kafka.describe_quorum())
        directory_id = next(voter["directoryId"] for voter in voters if voter["id"] == active_controller_id)
        self.kafka.controller_quorum.remove_controller(active_controller_id, directory_id)
        # Describe quorum output shows the second controller is now leader, old controller is an observer
        wait_until(lambda: check_describe_quorum_output(self.kafka.describe_quorum(),
                                                        inactive_controller_id,
                                                        [inactive_controller_id],
                                                        broker_only_ids + [active_controller_id]), timeout_sec=5)

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[combined_kraft])
    def test_combined_mode_reconfig(self, metadata_quorum):
        """
        Tests quorum reconfiguration in combined mode with produce & consume validation.
        Starts a controller in standalone mode with two other broker nodes, then calls perform_reconfig to add
        a second controller and then remove the first controller.
        """
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=4,  # 2 combined, 2 broker-only nodes
                                  zk=None,
                                  topics={self.topic: {"partitions": self.partitions,
                                                       "replication-factor": self.replication_factor,
                                                       'configs': {"min.insync.replicas": 1}}},
                                  version=DEV_BRANCH,
                                  controller_num_nodes_override=2,
                                  dynamicRaftQuorum=True)
        # Start a controller and the broker-only nodes
        # We leave starting the second controller for later in perform_reconfig
        inactive_controller = self.kafka.nodes[1]
        self.kafka.start(nodes_to_skip=[inactive_controller])

        # Start producer and consumer
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int, compression_types=["none"],
                                           version=DEV_BRANCH, offline_nodes=[inactive_controller])
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=DEV_BRANCH)
        # Perform reconfigurations
        self.run_produce_consume_validate(
            core_test_action=lambda: self.perform_reconfig(self.kafka.idx(self.kafka.nodes[0]),
                                                           self.kafka.idx(inactive_controller),
                                                           inactive_controller,
                                                           [self.kafka.idx(node) for node in self.kafka.nodes[2:]]))

    @cluster(num_nodes=7)
    @matrix(metadata_quorum=[isolated_kraft])
    def test_isolated_mode_reconfig(self, metadata_quorum):
        """
        Tests quorum reconfiguration in isolated mode with produce & consume validation.
        Starts a controller in standalone mode with three other broker nodes, then calls perform_reconfig to add
        a second controller and then remove the first controller.
        """
        # Start up KRaft controller in migration mode
        remote_quorum = partial(ServiceQuorumInfo, isolated_kraft)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,  # 3 broker-only nodes
                                  zk=None,
                                  topics={self.topic: {"partitions": self.partitions,
                                                       "replication-factor": self.replication_factor,
                                                       'configs': {"min.insync.replicas": 1}}},
                                  version=DEV_BRANCH,
                                  controller_num_nodes_override=2,
                                  quorum_info_provider=remote_quorum,
                                  dynamicRaftQuorum=True)
        # Start a controller and the broker-only nodes
        # We leave starting the second controller for later in perform_reconfig
        controller_quorum = self.kafka.controller_quorum
        inactive_controller = controller_quorum.nodes[1]
        self.kafka.start(isolated_controllers_to_skip=[inactive_controller])

        # Start producer and consumer
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int, compression_types=["none"],
                                           version=DEV_BRANCH)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=DEV_BRANCH)
        # Perform reconfigurations
        self.run_produce_consume_validate(
            core_test_action=lambda: self.perform_reconfig(controller_quorum.node_id_as_isolated_controller(self.kafka.controller_quorum.nodes[0]),
                                                           controller_quorum.node_id_as_isolated_controller(inactive_controller),
                                                           inactive_controller,
                                                           [self.kafka.idx(node) for node in self.kafka.nodes]))

def check_nodes_in_output(pattern: str, output: str, *node_ids: int):
    nodes = json_from_line(pattern, output)
    if len(nodes) != len(node_ids):
        return False

    for node in nodes:
        if not node["id"] in node_ids:
            return False
    return True

def check_describe_quorum_output(output: str, leader_id: int, voter_ids: List[int], observer_ids: List[int]):
    """
    Check that the describe quorum output contains the expected leader, voters, and observers
    :param output: Describe quorum output
    :param leader_id: Expected leader id
    :param voter_ids: Expected voter ids
    :param observer_ids: Expected observer ids
    :return:
    """
    if not re.search(r"LeaderId:\s*" + str(leader_id), output):
        return False
    return (check_nodes_in_output(r"CurrentVoters:.*", output, *voter_ids) and
            check_nodes_in_output(r"CurrentObservers:.*", output, *observer_ids))

def json_from_line(pattern: str, output: str):
    match = re.search(pattern, output)
    if not match:
        raise Exception(f"Expected match for pattern {pattern} in describe quorum output")
    line = match.group(0)
    start_index = line.find('[')
    end_index = line.rfind(']') + 1

    return json.loads(line[start_index:end_index])
