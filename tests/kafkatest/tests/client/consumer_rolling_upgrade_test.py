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
from ducktape.mark.resource import cluster


from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition, quorum

class ConsumerRollingUpgradeTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 4
    RANGE = "org.apache.kafka.clients.consumer.RangeAssignor"
    ROUND_ROBIN = "org.apache.kafka.clients.consumer.RoundRobinAssignor"

    def __init__(self, test_context):
        super(ConsumerRollingUpgradeTest, self).__init__(test_context, num_consumers=2, num_producers=0,
                                                         num_zk=1, num_brokers=1, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 }
        })

    def _verify_range_assignment(self, consumer):
        # range assignment should give us two partition sets: (0, 1) and (2, 3)
        assignment = set([frozenset(partitions) for partitions in consumer.current_assignment().values()])
        assert assignment == set([
            frozenset([TopicPartition(self.TOPIC, 0), TopicPartition(self.TOPIC, 1)]),
            frozenset([TopicPartition(self.TOPIC, 2), TopicPartition(self.TOPIC, 3)])]), \
            "Mismatched assignment: %s" % assignment

    def _verify_roundrobin_assignment(self, consumer):
        assignment = set([frozenset(x) for x in consumer.current_assignment().values()])
        assert assignment == set([
            frozenset([TopicPartition(self.TOPIC, 0), TopicPartition(self.TOPIC, 2)]),
            frozenset([TopicPartition(self.TOPIC, 1), TopicPartition(self.TOPIC, 3)])]), \
            "Mismatched assignment: %s" % assignment

    @cluster(num_nodes=4)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def rolling_update_test(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Verify rolling updates of partition assignment strategies works correctly. In this
        test, we use a rolling restart to change the group's assignment strategy from "range" 
        to "roundrobin." We verify after every restart that all members are still in the group
        and that the correct assignment strategy was used.
        """

        # initialize the consumer using range assignment
        consumer = self.setup_consumer(self.TOPIC, assignment_strategy=self.RANGE)

        consumer.start()
        self.await_all_members(consumer)
        self._verify_range_assignment(consumer)

        # change consumer configuration to prefer round-robin assignment, but still support range assignment
        consumer.assignment_strategy = self.ROUND_ROBIN + "," + self.RANGE

        # restart one of the nodes and verify that we are still using range assignment
        consumer.stop_node(consumer.nodes[0])
        consumer.start_node(consumer.nodes[0])
        self.await_all_members(consumer)
        self._verify_range_assignment(consumer)

        # now restart the other node and verify that we have switched to round-robin
        consumer.stop_node(consumer.nodes[1])
        consumer.start_node(consumer.nodes[1])
        self.await_all_members(consumer)
        self._verify_roundrobin_assignment(consumer)

        # if we want, we can now drop support for range assignment
        consumer.assignment_strategy = self.ROUND_ROBIN
        for node in consumer.nodes:
            consumer.stop_node(node)
            consumer.start_node(node)
            self.await_all_members(consumer)
            self._verify_roundrobin_assignment(consumer)
