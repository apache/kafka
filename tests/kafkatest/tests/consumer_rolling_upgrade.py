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

from ducktape.utils.util import wait_until

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.kafka import TopicPartition

class ConsumerRollingUpgradeTest(KafkaTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 4
    GROUP_ID = "test_group_id"
    RANGE = "org.apache.kafka.clients.consumer.RangeAssignor"
    ROUND_ROBIN = "org.apache.kafka.clients.consumer.RoundRobinAssignor"

    def __init__(self, test_context):
        super(ConsumerRollingUpgradeTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 }
        })
        self.num_consumers = 2
        self.session_timeout = 10000

    def min_cluster_size(self):
        return super(ConsumerRollingUpgradeTest, self).min_cluster_size() + self.num_consumers

    def _await_all_members(self, consumer):
        # Wait until all members have joined the group
        wait_until(lambda: len(consumer.joined_nodes()) == self.num_consumers, timeout_sec=self.session_timeout+5,
                   err_msg="Consumers failed to join in a reasonable amount of time")

    def _verify_range_assignment(self, consumer):
        # range assignment should give us two partition sets: (0, 1) and (2, 3)
        assignment = set([frozenset(partitions) for partitions in consumer.current_assignment().values()])
        assert assignment == set([
            frozenset([TopicPartition(self.TOPIC, 0), TopicPartition(self.TOPIC, 1)]),
            frozenset([TopicPartition(self.TOPIC, 2), TopicPartition(self.TOPIC, 3)])])

    def _verify_roundrobin_assignment(self, consumer):
        assignment = set([frozenset(x) for x in consumer.current_assignment().values()])
        assert assignment == set([
            frozenset([TopicPartition(self.TOPIC, 0), TopicPartition(self.TOPIC, 2)]),
            frozenset([TopicPartition(self.TOPIC, 1), TopicPartition(self.TOPIC, 3)])])

    def rolling_update_test(self):
        """
        Verify rolling updates of partition assignment strategies works correctly. In this
        test, we use a rolling restart to change the group's assignment strategy from "range" 
        to "roundrobin." We verify after every restart that all members are still in the group
        and that the correct assignment strategy was used.
        """

        # initialize the consumer using range assignment
        consumer = VerifiableConsumer(self.test_context, self.num_consumers, self.kafka,
                                      self.TOPIC, self.GROUP_ID, session_timeout=self.session_timeout,
                                      assignment_strategy=self.RANGE)
        consumer.start()
        self._await_all_members(consumer)
        self._verify_range_assignment(consumer)

        # change consumer configuration to prefer round-robin assignment, but still support range assignment
        consumer.assignment_strategy = self.ROUND_ROBIN + "," + self.RANGE

        # restart one of the nodes and verify that we are still using range assignment
        consumer.stop_node(consumer.nodes[0])
        consumer.start_node(consumer.nodes[0])
        self._await_all_members(consumer)
        self._verify_range_assignment(consumer)
        
        # now restart the other node and verify that we have switched to round-robin
        consumer.stop_node(consumer.nodes[1])
        consumer.start_node(consumer.nodes[1])
        self._await_all_members(consumer)
        self._verify_roundrobin_assignment(consumer)

        # if we want, we can now drop support for range assignment
        consumer.assignment_strategy = self.ROUND_ROBIN
        for node in consumer.nodes:
            consumer.stop_node(node)
            consumer.start_node(node)
            self._await_all_members(consumer)
            self._verify_roundrobin_assignment(consumer)
