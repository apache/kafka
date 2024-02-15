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
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.verifiable_consumer import ConsumerState, VerifiableConsumer
from kafkatest.services.kafka import TopicPartition


class VerifiableConsumerTest(KafkaTest):
    PRODUCER_REQUEST_TIMEOUT_SEC = 30

    def __init__(self, test_context, num_consumers=1, num_producers=0,
                 group_id="test_group_id", session_timeout_sec=10, **kwargs):
        super(VerifiableConsumerTest, self).__init__(test_context, **kwargs)
        self.num_consumers = num_consumers
        self.num_producers = num_producers
        self.group_id = group_id
        self.session_timeout_sec = session_timeout_sec
        self.consumption_timeout_sec = max(self.PRODUCER_REQUEST_TIMEOUT_SEC + 5, 2 * session_timeout_sec)

    def _all_partitions(self, topic, num_partitions):
        partitions = set()
        for i in range(num_partitions):
            partitions.add(TopicPartition(topic=topic, partition=i))
        return partitions

    def _partitions(self, assignment):
        partitions = []
        for parts in assignment.values():
            partitions += parts
        return partitions

    def await_valid_assignment(self, consumer, topic, num_partitions, num_consumers):
        # Wait until all assignments have settled
        timeout_sec = self.session_timeout_sec * 2
        wait_until(lambda: self._valid_assignment(topic, num_partitions, consumer.current_assignment()),
                   timeout_sec=timeout_sec,
                   err_msg=self._valid_assignment_err_msg(consumer, topic, num_consumers, num_partitions, timeout_sec))

    def _valid_assignment(self, topic, num_partitions, assignment):
        all_partitions = self._all_partitions(topic, num_partitions)
        partitions = self._partitions(assignment)
        return len(partitions) == num_partitions and set(partitions) == all_partitions

    def _valid_assignment_err_msg(self, consumer, topic, num_consumers, num_partitions, timeout_sec):
        assignment = consumer.current_assignment()
        assignments = [(str(node.account), a) for node, a in assignment.items()]
        return "Not all of the %d partitions for topic %s were assigned among the %d consumers within the timeout of %d seconds: %s" % (num_partitions, topic, num_consumers, timeout_sec, assignments),


    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(VerifiableConsumerTest, self).min_cluster_size() + self.num_consumers + self.num_producers

    def setup_consumer(self, topic, static_membership=False, enable_autocommit=False,
                       assignment_strategy="org.apache.kafka.clients.consumer.RangeAssignor",
                       group_remote_assignor="range",
                       **kwargs):
        return VerifiableConsumer(self.test_context, self.num_consumers, self.kafka,
                                  topic, self.group_id, static_membership=static_membership, session_timeout_sec=self.session_timeout_sec,
                                  assignment_strategy=assignment_strategy, enable_autocommit=enable_autocommit,
                                  group_remote_assignor=group_remote_assignor,
                                  log_level="TRACE", **kwargs)

    def setup_producer(self, topic, max_messages=-1, throughput=500):
        return VerifiableProducer(self.test_context, self.num_producers, self.kafka, topic,
                                  max_messages=max_messages, throughput=throughput,
                                  request_timeout_sec=self.PRODUCER_REQUEST_TIMEOUT_SEC,
                                  log_level="DEBUG")

    def await_produced_messages(self, producer, min_messages=1000, timeout_sec=10):
        current_acked = producer.num_acked
        wait_until(lambda: producer.num_acked >= current_acked + min_messages, timeout_sec=timeout_sec,
                   err_msg="Timeout awaiting messages to be produced and acked")

    def await_consumed_messages(self, consumer, min_messages=1):
        timeout_sec = self.consumption_timeout_sec
        current_total = consumer.total_consumed()
        lower_bound = current_total + min_messages
        wait_until(lambda: consumer.total_consumed() >= lower_bound,
                   timeout_sec=timeout_sec,
                   err_msg=self._await_consumed_messages_err_msg(consumer, lower_bound, timeout_sec))

    def _await_consumed_messages_err_msg(self, consumer, lower_bound, timeout_sec):
        actual = consumer.total_consumed()
        return "Consumers received only %d out of %d expected messages within the timeout of %d seconds" % (actual, lower_bound, timeout_sec)

    def await_members(self, consumer, num_consumers, require_joined=True):
        # Wait until all members have joined the group
        timeout_sec = self.session_timeout_sec * 2
        wait_until(lambda: self._await_members(consumer, require_joined) == num_consumers,
                   timeout_sec=timeout_sec,
                   err_msg=self._await_members_err_msg(consumer, num_consumers, require_joined, timeout_sec))

    def _await_members(self, consumer, require_joined):
        count = len(consumer.joined_nodes())

        if not require_joined:
            count += len(consumer.started_nodes())
            count += len(consumer.rebalancing_nodes())

        return count

    def _await_members_err_msg(self, consumer, num_consumers, require_joined, timeout_sec):
        actual = self._await_members(consumer, require_joined)
        return "Only %d out of %d consumers joined the group within the timeout of %d seconds" % (actual, num_consumers, timeout_sec)

    def await_all_members(self, consumer, require_joined=True):
        self.await_members(consumer, self.num_consumers, require_joined)
