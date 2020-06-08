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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition
from kafkatest.services.verifiable_consumer import VerifiableConsumer


class TruncationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 1
    TOPICS = {
        TOPIC: {
            'partitions': NUM_PARTITIONS,
            'replication-factor': 2
        }
    }
    GROUP_ID = "truncation-test"

    def __init__(self, test_context):
        super(TruncationTest, self).__init__(test_context, num_consumers=1, num_producers=1,
                                             num_zk=1, num_brokers=3, topics=self.TOPICS)
        self.last_total = 0
        self.all_offsets_consumed = []
        self.all_values_consumed = []

    def setup_consumer(self, topic, **kwargs):
        consumer = super(TruncationTest, self).setup_consumer(topic, **kwargs)
        self.mark_for_collect(consumer, 'verifiable_consumer_stdout')

        def print_record(event, node):
            self.all_offsets_consumed.append(event['offset'])
            self.all_values_consumed.append(event['value'])
        consumer.on_record_consumed = print_record

        return consumer

    @cluster(num_nodes=7)
    def test_offset_truncate(self):
        """
        Verify correct consumer behavior when the brokers are consecutively restarted.

        Setup: single Kafka cluster with one producer writing messages to a single topic with one
        partition, an a set of consumers in the same group reading from the same topic.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each broker consecutively, waiting for the group to stabilize between
          each broker restart.
        - Verify delivery semantics according to the failure type and that the broker bounces
          did not cause unexpected group rebalances.
        """
        tp = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC, throughput=10)
        producer.start()
        self.await_produced_messages(producer, min_messages=10)

        consumer = self.setup_consumer(self.TOPIC, reset_policy="earliest", verify_offsets=False)
        consumer.start()
        self.await_all_members(consumer)

        # Reduce ISR to one node
        isr = self.kafka.isr_idx_list(self.TOPIC, 0)
        node1 = self.kafka.get_node(isr[0])
        self.kafka.stop_node(node1)
        self.logger.info("Reduced ISR to one node, consumer is at %s", consumer.current_position(tp))

        # Ensure remaining ISR member has a little bit of data
        current_total = consumer.total_consumed()
        wait_until(lambda: consumer.total_consumed() > current_total + 10,
                   timeout_sec=30,
                   err_msg="Timed out waiting for consumer to move ahead by 10 messages")

        # Kill last ISR member
        node2 = self.kafka.get_node(isr[1])
        self.kafka.stop_node(node2)
        self.logger.info("No members in ISR, consumer is at %s", consumer.current_position(tp))

        # Keep consuming until we've caught up to HW
        def none_consumed(this, consumer):
            new_total = consumer.total_consumed()
            if new_total == this.last_total:
                return True
            else:
                this.last_total = new_total
                return False

        self.last_total = consumer.total_consumed()
        wait_until(lambda: none_consumed(self, consumer),
                   timeout_sec=30,
                   err_msg="Timed out waiting for the consumer to catch up")

        self.kafka.start_node(node1)
        self.logger.info("Out of sync replica is online, but not electable. Consumer is at  %s", consumer.current_position(tp))

        pre_truncation_pos = consumer.current_position(tp)

        self.kafka.set_unclean_leader_election(self.TOPIC)
        self.logger.info("New unclean leader, consumer is at %s", consumer.current_position(tp))

        # Wait for truncation to be detected
        self.kafka.start_node(node2)
        wait_until(lambda: consumer.current_position(tp) >= pre_truncation_pos,
                   timeout_sec=30,
                   err_msg="Timed out waiting for truncation")

        # Make sure we didn't reset to beginning of log
        total_records_consumed = len(self.all_values_consumed)
        assert total_records_consumed == len(set(self.all_values_consumed)), "Received duplicate records"

        consumer.stop()
        producer.stop()

        # Re-consume all the records
        consumer2 = VerifiableConsumer(self.test_context, 1, self.kafka, self.TOPIC, group_id="group2",
                                       reset_policy="earliest", verify_offsets=True)

        consumer2.start()
        self.await_all_members(consumer2)

        wait_until(lambda: consumer2.total_consumed() > 0,
           timeout_sec=30,
           err_msg="Timed out waiting for consumer to consume at least 10 messages")

        self.last_total = consumer2.total_consumed()
        wait_until(lambda: none_consumed(self, consumer2),
               timeout_sec=30,
               err_msg="Timed out waiting for the consumer to fully consume data")

        second_total_consumed = consumer2.total_consumed()
        assert second_total_consumed < total_records_consumed, "Expected fewer records with new consumer since we truncated"
        self.logger.info("Second consumer saw only %s, meaning %s were truncated",
                         second_total_consumed, total_records_consumed - second_total_consumed)