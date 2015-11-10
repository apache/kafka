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
from ducktape.utils.util import wait_until

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.kafka import TopicPartition

import signal

def partitions_for(topic, num_partitions):
    partitions = set()
    for i in range(num_partitions):
        partitions.add(TopicPartition(topic=topic, partition=i))
    return partitions

class VerifiableConsumerTest(KafkaTest):
    STOPIC = "simple_topic"
    TOPIC = "test_topic"
    NUM_PARTITIONS = 3
    PARTITIONS = partitions_for(TOPIC, NUM_PARTITIONS)
    GROUP_ID = "test_group_id"

    def __init__(self, test_context):
        super(VerifiableConsumerTest, self).__init__(test_context, num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 },
            self.STOPIC : { 'partitions': 1, 'replication-factor': 2 }
        })
        self.num_producers = 1
        self.num_consumers = 2
        self.session_timeout = 10000

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(VerifiableConsumerTest, self).min_cluster_size() + self.num_consumers + self.num_producers

    def _partitions(self, assignment):
        partitions = []
        for parts in assignment.itervalues():
            partitions += parts
        return partitions

    def _valid_assignment(self, assignment):
        partitions = self._partitions(assignment)
        return len(partitions) == self.NUM_PARTITIONS and set(partitions) == self.PARTITIONS

    def _setup_consumer(self, topic, enable_autocommit=False):
        return VerifiableConsumer(self.test_context, self.num_consumers, self.kafka,
                                  topic, self.GROUP_ID, session_timeout=self.session_timeout,
                                  enable_autocommit=enable_autocommit)

    def _setup_producer(self, topic, max_messages=-1):
        return VerifiableProducer(self.test_context, self.num_producers,
                                  self.kafka, topic, max_messages=max_messages)

    def _await_all_members(self, consumer):
        # Wait until all members have joined the group
        wait_until(lambda: len(consumer.joined_nodes()) == self.num_consumers, timeout_sec=10,
                   err_msg="Consumers failed to join in a reasonable amount of time")

    @matrix(clean_shutdown=[True, False], enable_autocommit=[True, False])
    def test_consumer_failure(self, clean_shutdown, enable_autocommit):
        partition = TopicPartition(self.STOPIC, 0)
        
        consumer = self._setup_consumer(self.STOPIC, enable_autocommit=enable_autocommit)
        producer = self._setup_producer(self.STOPIC)

        consumer.start()
        self._await_all_members(consumer)

        partition_owner = consumer.owner(partition)
        assert partition_owner is not None

        # startup the producer and ensure that some records have been written
        producer.start()
        wait_until(lambda: producer.num_acked > 1000, timeout_sec=10,
                   err_msg="Producer failed waiting for messages to be written")

        # stop the partition owner and await its shutdown
        consumer.kill_node(partition_owner, clean_shutdown=clean_shutdown)
        wait_until(lambda: len(consumer.joined_nodes()) == 1 and consumer.owner(partition) != None,
                   timeout_sec=self.session_timeout+5, err_msg="Timed out waiting for consumer to close")

        # ensure that the remaining consumer does some work after rebalancing
        current_total_consumed = consumer.total_consumed()
        wait_until(lambda: consumer.total_consumed() > current_total_consumed + 1000, timeout_sec=10,
                   err_msg="Timed out waiting for additional records to be consumed after first consumer failed")

        consumer.stop_all()

        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records did not match consumed position"
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position greater than the total number of consumed records"

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset did not match last consumed position"


    @matrix(clean_shutdown=[True, False], enable_autocommit=[True, False])
    def test_broker_failure(self, clean_shutdown, enable_autocommit):
        partition = TopicPartition(self.STOPIC, 0)
        
        consumer = self._setup_consumer(self.STOPIC, enable_autocommit=enable_autocommit)
        producer = self._setup_producer(self.STOPIC)

        producer.start()
        consumer.start()
        self._await_all_members(consumer)

        num_rebalances = consumer.num_rebalances()

        # shutdown one of the brokers
        # TODO: we need a way to target the coordinator instead of picking arbitrarily
        self.kafka.signal_node(self.kafka.nodes[0], signal.SIGTERM if clean_shutdown else signal.SIGKILL)

        # ensure that the consumers do some work after the broker failure
        current_total_consumed = consumer.total_consumed()
        wait_until(lambda: consumer.total_consumed() > current_total_consumed + 1000, timeout_sec=20,
                   err_msg="Timed out waiting for additional records to be consumed after first consumer failed")

        # verify that there were no rebalances on failover
        assert num_rebalances == consumer.num_rebalances(), "Broker failure should not cause a rebalance"

        consumer.stop_all()

        # if the total records consumed matches the current position, we haven't seen any duplicates
        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records did not match consumed position"

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset did not match last consumed position"

    def test_simple_consume(self):
        total_records = 1000

        consumer = self._setup_consumer(self.STOPIC)
        producer = self._setup_producer(self.STOPIC, max_messages=total_records)

        partition = TopicPartition(self.STOPIC, 0)

        consumer.start()
        self._await_all_members(consumer)

        producer.start()
        wait_until(lambda: producer.num_acked == total_records, timeout_sec=20,
                   err_msg="Producer failed waiting for messages to be written")

        wait_until(lambda: consumer.last_commit(partition) == total_records, timeout_sec=10,
                   err_msg="Consumer failed to read all expected messages")

        assert consumer.current_position(partition) == total_records

    def test_valid_assignment(self):
        consumer = self._setup_consumer(self.TOPIC)
        consumer.start()
        self._await_all_members(consumer)
        assert self._valid_assignment(consumer.current_assignment())
