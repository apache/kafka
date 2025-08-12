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

import time
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.trogdor.network_partition_fault_spec import NetworkPartitionFaultSpec
from kafkatest.services.trogdor.degraded_network_fault_spec import DegradedNetworkFaultSpec
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.trogdor.process_stop_fault_spec import ProcessStopFaultSpec
from kafkatest.services.trogdor.round_trip_workload import RoundTripWorkloadService, RoundTripWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService


class RoundTripFaultTest(Test):
    topic_name_index = 0

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(RoundTripFaultTest, self).__init__(test_context)
        self.kafka = KafkaService(test_context, num_nodes=4, zk=None)
        self.workload_service = RoundTripWorkloadService(test_context, self.kafka)
        if quorum.for_test(test_context) == quorum.isolated_kraft:
            trogdor_client_services = [self.kafka.controller_quorum, self.kafka, self.workload_service]
        else: #co-located case, which we currently don't test but handle here for completeness in case we do test it
            trogdor_client_services = [self.kafka, self.workload_service]
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=trogdor_client_services)
        topic_name = "round_trip_topic%d" % RoundTripFaultTest.topic_name_index
        RoundTripFaultTest.topic_name_index = RoundTripFaultTest.topic_name_index + 1
        # note that the broker.id values will be 1..num_nodes
        active_topics={topic_name : {"partitionAssignments":{"0": [1,2,3]}}}
        self.round_trip_spec = RoundTripWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                     self.workload_service.client_node,
                                     self.workload_service.bootstrap_servers,
                                     target_messages_per_sec=10000,
                                     max_messages=100000,
                                     active_topics=active_topics)

    def setUp(self):
        self.kafka.start()
        self.trogdor.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()

    def remote_quorum_nodes(self):
        if quorum.for_test(self.test_context) == quorum.isolated_kraft:
            return self.kafka.controller_quorum.nodes
        else: # co-located case, which we currently don't test but handle here for completeness in case we do test it
            return []

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_round_trip_workload(self, metadata_quorum):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        workload1.wait_for_done(timeout_sec=600)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_round_trip_workload_with_broker_partition(self, metadata_quorum):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        part1 = [self.kafka.nodes[0]]
        part2 = self.kafka.nodes[1:] + [self.workload_service.nodes[0]] + self.remote_quorum_nodes()
        partition1_spec = NetworkPartitionFaultSpec(0, TaskSpec.MAX_DURATION_MS,
                                                    [part1, part2])
        partition1 = self.trogdor.create_task("partition1", partition1_spec)
        workload1.wait_for_done(timeout_sec=600)
        partition1.stop()
        partition1.wait_for_done()

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_produce_consume_with_broker_pause(self, metadata_quorum):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        stop1_spec = ProcessStopFaultSpec(0, TaskSpec.MAX_DURATION_MS, [self.kafka.nodes[0]],
                                           self.kafka.java_class_name())
        stop1 = self.trogdor.create_task("stop1", stop1_spec)
        workload1.wait_for_done(timeout_sec=600)
        stop1.stop()
        stop1.wait_for_done()
        self.kafka.stop_node(self.kafka.nodes[0], False)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_produce_consume_with_client_partition(self, metadata_quorum):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        part1 = [self.workload_service.nodes[0]]
        part2 = self.kafka.nodes + self.remote_quorum_nodes()
        partition1_spec = NetworkPartitionFaultSpec(0, 60000, [part1, part2])
        stop1 = self.trogdor.create_task("stop1", partition1_spec)
        workload1.wait_for_done(timeout_sec=600)
        stop1.stop()
        stop1.wait_for_done()

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_produce_consume_with_latency(self, metadata_quorum):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        spec = DegradedNetworkFaultSpec(0, 60000)
        for node in self.kafka.nodes + self.remote_quorum_nodes():
            spec.add_node_spec(node.name, "eth0", latencyMs=100, rateLimitKbit=3000)
        slow1 = self.trogdor.create_task("slow1", spec)
        workload1.wait_for_done(timeout_sec=600)
        slow1.stop()
        slow1.wait_for_done()
