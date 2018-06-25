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
from ducktape.tests.test import Test
from kafkatest.services.trogdor.network_partition_fault_spec import NetworkPartitionFaultSpec
from kafkatest.services.kafka import KafkaService
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
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.kafka = KafkaService(test_context, num_nodes=4, zk=self.zk)
        self.workload_service = RoundTripWorkloadService(test_context, self.kafka)
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.zk, self.kafka, self.workload_service])
        topic_name = "round_trip_topic%d" % RoundTripFaultTest.topic_name_index
        RoundTripFaultTest.topic_name_index = RoundTripFaultTest.topic_name_index + 1
        active_topics={topic_name : {"partitionAssignments":{"0": [0,1,2]}}}
        self.round_trip_spec = RoundTripWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                     self.workload_service.client_node,
                                     self.workload_service.bootstrap_servers,
                                     target_messages_per_sec=10000,
                                     max_messages=100000,
                                     active_topics=active_topics)

    def setUp(self):
        self.zk.start()
        self.kafka.start()
        self.trogdor.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()
        self.zk.stop()

    def test_round_trip_workload(self):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        workload1.wait_for_done(timeout_sec=600)

    def test_round_trip_workload_with_broker_partition(self):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        part1 = [self.kafka.nodes[0]]
        part2 = self.kafka.nodes[1:] + [self.workload_service.nodes[0]] + self.zk.nodes
        partition1_spec = NetworkPartitionFaultSpec(0, TaskSpec.MAX_DURATION_MS,
                                                    [part1, part2])
        partition1 = self.trogdor.create_task("partition1", partition1_spec)
        workload1.wait_for_done(timeout_sec=600)
        partition1.stop()
        partition1.wait_for_done()

    def test_produce_consume_with_broker_pause(self):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        stop1_spec = ProcessStopFaultSpec(0, TaskSpec.MAX_DURATION_MS, [self.kafka.nodes[0]],
                                           self.kafka.java_class_name())
        stop1 = self.trogdor.create_task("stop1", stop1_spec)
        workload1.wait_for_done(timeout_sec=600)
        stop1.stop()
        stop1.wait_for_done()
        self.kafka.stop_node(self.kafka.nodes[0], False)

    def test_produce_consume_with_client_partition(self):
        workload1 = self.trogdor.create_task("workload1", self.round_trip_spec)
        time.sleep(2)
        part1 = [self.workload_service.nodes[0]]
        part2 = self.kafka.nodes + self.zk.nodes
        partition1_spec = NetworkPartitionFaultSpec(0, 60000, [part1, part2])
        stop1 = self.trogdor.create_task("stop1", partition1_spec)
        workload1.wait_for_done(timeout_sec=600)
        stop1.stop()
        stop1.wait_for_done()
