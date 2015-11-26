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
        return VerifiableProducer(self.test_context, self.num_producers, self.kafka, topic,
                                  max_messages=max_messages, throughput=500)

    def _await_all_members(self, consumer):
        # Wait until all members have joined the group
        wait_until(lambda: len(consumer.joined_nodes()) == self.num_consumers, timeout_sec=self.session_timeout+5,
                   err_msg="Consumers failed to join in a reasonable amount of time")

    def rolling_bounce_consumers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

                wait_until(lambda: len(consumer.dead_nodes()) == (self.num_consumers - 1), timeout_sec=self.session_timeout,
                           err_msg="Timed out waiting for the consumers to shutdown")

                total_consumed = consumer.total_consumed()
            
                consumer.start_node(node)

                wait_until(lambda: len(consumer.joined_nodes()) == self.num_consumers and consumer.total_consumed() > total_consumed,
                           timeout_sec=self.session_timeout,
                           err_msg="Timed out waiting for the consumers to shutdown")

    def bounce_all_consumers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers, timeout_sec=10,
                       err_msg="Timed out waiting for the consumers to shutdown")

            total_consumed = consumer.total_consumed()
            
            for node in consumer.nodes:
                consumer.start_node(node)

            wait_until(lambda: len(consumer.joined_nodes()) == self.num_consumers and consumer.total_consumed() > total_consumed,
                       timeout_sec=self.session_timeout*2,
                       err_msg="Timed out waiting for the consumers to shutdown")

    def rolling_bounce_brokers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in self.kafka.nodes:
                total_consumed = consumer.total_consumed()

                self.kafka.restart_node(node, clean_shutdown=True)

                wait_until(lambda: len(consumer.joined_nodes()) == self.num_consumers and consumer.total_consumed() > total_consumed,
                           timeout_sec=30,
                           err_msg="Timed out waiting for the broker to shutdown")

    def bounce_all_brokers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in self.kafka.nodes:
                self.kafka.stop_node(node)

            for node in self.kafka.nodes:
                self.kafka.start_node(node)
            

    def test_broker_rolling_bounce(self):
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
        partition = TopicPartition(self.STOPIC, 0)
        
        producer = self._setup_producer(self.STOPIC)
        consumer = self._setup_consumer(self.STOPIC)

        producer.start()
        wait_until(lambda: producer.num_acked > 1000, timeout_sec=10,
                   err_msg="Producer failed waiting for messages to be written")

        consumer.start()
        self._await_all_members(consumer)

        num_rebalances = consumer.num_rebalances()
        # TODO: make this test work with hard shutdowns, which probably requires
        #       pausing before the node is restarted to ensure that any ephemeral
        #       nodes have time to expire
        self.rolling_bounce_brokers(consumer, clean_shutdown=True)
        
        unexpected_rebalances = consumer.num_rebalances() - num_rebalances
        assert unexpected_rebalances == 0, \
            "Broker rolling bounce caused %d unexpected group rebalances" % unexpected_rebalances

        consumer.stop_all()

        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records did not match consumed position"

    @matrix(clean_shutdown=[True, False], bounce_mode=["all", "rolling"])
    def test_consumer_bounce(self, clean_shutdown, bounce_mode):
        """
        Verify correct consumer behavior when the consumers in the group are consecutively restarted.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each consumer, waiting for each one to rejoin the group before
          restarting the rest.
        - Verify delivery semantics according to the failure type.
        """
        partition = TopicPartition(self.STOPIC, 0)
        
        producer = self._setup_producer(self.STOPIC)
        consumer = self._setup_consumer(self.STOPIC)

        producer.start()
        wait_until(lambda: producer.num_acked > 1000, timeout_sec=10,
                   err_msg="Producer failed waiting for messages to be written")

        consumer.start()
        self._await_all_members(consumer)

        if bounce_mode == "all":
            self.bounce_all_consumers(consumer, clean_shutdown=clean_shutdown)
        else:
            self.rolling_bounce_consumers(consumer, clean_shutdown=clean_shutdown)
                
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
        wait_until(lambda: len(consumer.joined_nodes()) == (self.num_consumers - 1) and consumer.owner(partition) != None,
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
