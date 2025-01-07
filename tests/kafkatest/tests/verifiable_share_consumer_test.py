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
from kafkatest.services.verifiable_share_consumer import VerifiableShareConsumer
from kafkatest.services.kafka import TopicPartition

class VerifiableShareConsumerTest(KafkaTest):
    PRODUCER_REQUEST_TIMEOUT_SEC = 30

    def __init__(self, test_context, num_consumers=1, num_producers=0, **kwargs):
        super(VerifiableShareConsumerTest, self).__init__(test_context, **kwargs)
        self.num_consumers = num_consumers
        self.num_producers = num_producers

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

    def valid_assignment(self, topic, num_partitions, assignment):
        all_partitions = self._all_partitions(topic, num_partitions)
        partitions = self._partitions(assignment)
        return len(partitions) == num_partitions and set(partitions) == all_partitions

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(VerifiableShareConsumerTest, self).min_cluster_size() + self.num_consumers + self.num_producers

    def setup_share_group(self, topic, acknowledgement_mode="auto", group_id="test_group_id", offset_reset_strategy="", **kwargs):
        return VerifiableShareConsumer(self.test_context, self.num_consumers, self.kafka,
                                  topic, group_id, acknowledgement_mode=acknowledgement_mode,
                                  offset_reset_strategy=offset_reset_strategy, log_level="TRACE", **kwargs)

    def setup_producer(self, topic, max_messages=-1, throughput=500):
        return VerifiableProducer(self.test_context, self.num_producers, self.kafka, topic,
                                  max_messages=max_messages, throughput=throughput,
                                  request_timeout_sec=self.PRODUCER_REQUEST_TIMEOUT_SEC,
                                  log_level="DEBUG")

    def await_produced_messages(self, producer, min_messages=1000, timeout_sec=10):
        current_acked = producer.num_acked
        wait_until(lambda: producer.num_acked >= current_acked + min_messages, timeout_sec=timeout_sec,
                   err_msg="Timeout awaiting messages to be produced and acked")

    def await_consumed_messages(self, consumer, min_messages=1, timeout_sec=10, total=False):
        current_total = 0
        if total is False:
            current_total = consumer.total_consumed()
        wait_until(lambda: consumer.total_consumed() >= current_total + min_messages,
                   timeout_sec=timeout_sec,
                   err_msg="Timed out waiting for consumption")
        
    def await_unique_consumed_messages(self, consumer, min_messages=1, timeout_sec=10):
        wait_until(lambda: consumer.total_unique_consumed() >= min_messages,
                   timeout_sec=timeout_sec,
                   err_msg="Timed out waiting for consumption")

    def await_acknowledged_messages(self, consumer, min_messages=1, timeout_sec=10):
        wait_until(lambda: consumer.total_acknowledged() >=  min_messages,
                   timeout_sec=timeout_sec,
                   err_msg="Timed out waiting for consumption")
        
    def await_unique_acknowledged_messages(self, consumer, min_messages=1, timeout_sec=10):
        wait_until(lambda: consumer.total_unique_acknowledged() >=  min_messages,
                   timeout_sec=timeout_sec,
                   err_msg="Timed out waiting for consumption")

    def await_members(self, consumer, num_consumers, timeout_sec=10):
        # Wait until all members have started
        wait_until(lambda: len(consumer.alive_nodes()) == num_consumers,
                   timeout_sec=timeout_sec,
                   err_msg="Consumers failed to start in a reasonable amount of time")

    def await_all_members(self, consumer, timeout_sec=10):
        self.await_members(consumer, self.num_consumers, timeout_sec=timeout_sec)