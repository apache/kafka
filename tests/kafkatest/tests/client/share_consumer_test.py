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
from ducktape.utils.util import wait_until
from kafkatest.tests.verifiable_share_consumer_test import VerifiableShareConsumerTest

from kafkatest.services.kafka import TopicPartition, quorum

import signal

class ShareConsumerTest(VerifiableShareConsumerTest):
    TOPIC1 = {"name": "test_topic1", "partitions": 1,"replication_factor": 1}
    TOPIC2 = {"name": "test_topic2", "partitions": 3,"replication_factor": 3}
    TOPIC3 = {"name": "test_topic3", "partitions": 3,"replication_factor": 3}

    num_consumers = 3
    num_producers = 1
    num_brokers = 3

    default_timeout_sec = 600

    def __init__(self, test_context):
        super(ShareConsumerTest, self).__init__(test_context, num_consumers=self.num_consumers, num_producers=self.num_producers,
                                        num_zk=0, num_brokers=self.num_brokers, topics={
                self.TOPIC1["name"] : { 'partitions': self.TOPIC1["partitions"], 'replication-factor': self.TOPIC1["replication_factor"] },
                self.TOPIC2["name"] : { 'partitions': self.TOPIC2["partitions"], 'replication-factor': self.TOPIC2["replication_factor"] }
            })

    def setup_share_group(self, topic, **kwargs):
        consumer = super(ShareConsumerTest, self).setup_share_group(topic, **kwargs)
        self.mark_for_collect(consumer, 'verifiable_share_consumer_stdout')
        return consumer

    def get_topic_partitions(self, topic):
        return [TopicPartition(topic["name"], i) for i in range(topic["partitions"])]

    def wait_until_topic_replicas_settled(self, topic, expected_num_isr, timeout_sec=default_timeout_sec):
        for partition in range(0, topic["partitions"]):
            wait_until(lambda: len(self.kafka.isr_idx_list(topic["name"], partition)) == expected_num_isr,
                       timeout_sec=timeout_sec, backoff_sec=1, err_msg="the expected number of ISRs did not settle in a reasonable amount of time")

    def wait_until_topic_partition_leaders_settled(self, topic, timeout_sec=default_timeout_sec):
        def leader_settled(partition_leader, topicName, partition):
            try:
                partition_leader(topicName, partition)
                return True
            except Exception:
                return False
        for partition in range(0, topic["partitions"]):
            wait_until(lambda: leader_settled(self.kafka.leader, topic["name"], partition),
                       timeout_sec=timeout_sec, backoff_sec=1, err_msg="partition leaders did not settle in a reasonable amount of time")

    def rolling_bounce_brokers(self, topic, num_bounces=5, clean_shutdown=True, timeout_sec=default_timeout_sec):
        for _ in range(num_bounces):
            for i in range(len(self.kafka.nodes)):
                node = self.kafka.nodes[i]
                self.kafka.restart_node(node, clean_shutdown=clean_shutdown)
                self.wait_until_topic_replicas_settled(topic, expected_num_isr = topic["replication_factor"], timeout_sec=timeout_sec)

    def fail_brokers(self, topic, num_brokers=1, clean_shutdown=True, timeout_sec=default_timeout_sec):
        for i in range(num_brokers):
            self.kafka.signal_node(self.kafka.nodes[i], signal.SIGTERM if clean_shutdown else signal.SIGKILL)
            self.wait_until_topic_replicas_settled(topic, topic["replication_factor"] - (i + 1))
        self.wait_until_topic_partition_leaders_settled(topic, timeout_sec=timeout_sec)

    def rolling_bounce_share_consumers(self, consumer, keep_alive=0, num_bounces=5, clean_shutdown=True, timeout_sec=default_timeout_sec):
        for _ in range(num_bounces):
            num_consumers_killed = 0
            for node in consumer.nodes[keep_alive:]:
                consumer.stop_node(node, clean_shutdown)
                num_consumers_killed += 1
                wait_until(lambda: len(consumer.dead_nodes()) == 1,
                           timeout_sec=timeout_sec,
                           err_msg="Timed out waiting for the share consumer to shutdown")

                consumer.start_node(node)

                self.await_all_members(consumer, timeout_sec=timeout_sec)
                self.await_consumed_messages_by_a_consumer(consumer, node, timeout_sec=timeout_sec)

    def bounce_all_share_consumers(self, consumer, keep_alive=0, num_bounces=5, clean_shutdown=True, timeout_sec=default_timeout_sec):
        for _ in range(num_bounces):
            for node in consumer.nodes[keep_alive:]:
                consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers - keep_alive, timeout_sec=timeout_sec,
                       err_msg="Timed out waiting for the share consumers to shutdown")

            num_alive_consumers = keep_alive
            for node in consumer.nodes[keep_alive:]:
                consumer.start_node(node)
                num_alive_consumers += 1
                self.await_members(consumer, num_consumers=num_alive_consumers, timeout_sec=timeout_sec)
                self.await_consumed_messages_by_a_consumer(consumer, node, timeout_sec=timeout_sec)

    def fail_share_consumers(self, consumer, num_consumers=1, clean_shutdown=True, timeout_sec=default_timeout_sec):
        for i in range(num_consumers):
            consumer.kill_node(consumer.nodes[i], clean_shutdown=clean_shutdown)
            wait_until(lambda: len(consumer.dead_nodes()) == (i + 1),
                       timeout_sec=timeout_sec,
                       err_msg="Timed out waiting for the share consumer to be killed")

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft]
    )
    def test_share_single_topic_partition(self, metadata_quorum=quorum.isolated_kraft):

        total_messages = 100000
        producer = self.setup_producer(self.TOPIC1["name"], max_messages=total_messages)

        consumer = self.setup_share_group(self.TOPIC1["name"], offset_reset_strategy="earliest")

        producer.start()

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        self.await_acknowledged_messages(consumer, min_messages=total_messages, timeout_sec=self.default_timeout_sec)

        assert consumer.total_consumed() >= producer.num_acked
        assert consumer.total_acknowledged_successfully() == producer.num_acked

        for event_handler in consumer.event_handlers.values():
            assert event_handler.total_consumed > 0
            assert event_handler.total_acknowledged_successfully > 0

        producer.stop()
        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft]
    )
    def test_share_multiple_partitions(self, metadata_quorum=quorum.isolated_kraft):

        total_messages = 1000000
        producer = self.setup_producer(self.TOPIC2["name"], max_messages=total_messages, throughput=5000)

        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        producer.start()

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        self.await_acknowledged_messages(consumer, min_messages=total_messages, timeout_sec=self.default_timeout_sec)

        assert consumer.total_consumed() >= producer.num_acked
        assert consumer.total_acknowledged_successfully() == producer.num_acked

        for event_handler in consumer.event_handlers.values():
            assert event_handler.total_consumed > 0
            assert event_handler.total_acknowledged_successfully > 0
            for topic_partition in self.get_topic_partitions(self.TOPIC2):
                assert topic_partition in event_handler.consumed_per_partition
                assert event_handler.consumed_per_partition[topic_partition] > 0
                assert topic_partition in event_handler.acknowledged_per_partition
                assert event_handler.acknowledged_per_partition[topic_partition] > 0

        producer.stop()
        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        clean_shutdown=[True, False],
        metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft]
    )
    def test_broker_rolling_bounce(self, clean_shutdown, metadata_quorum=quorum.isolated_kraft):

        producer = self.setup_producer(self.TOPIC2["name"])
        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        producer.start()
        self.await_produced_messages(producer, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        self.await_consumed_messages(consumer, timeout_sec=self.default_timeout_sec)
        self.rolling_bounce_brokers(self.TOPIC2, num_bounces=1, clean_shutdown=clean_shutdown, timeout_sec=self.default_timeout_sec)

        # ensure that the share consumers do some work after the broker bounces
        self.await_consumed_messages(consumer, min_messages=1000, timeout_sec=self.default_timeout_sec)

        producer.stop()

        self.await_unique_consumed_messages(consumer, min_messages=producer.num_acked, timeout_sec=self.default_timeout_sec)

        assert consumer.total_unique_consumed() >= producer.num_acked

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        clean_shutdown=[True, False],
        metadata_quorum=[quorum.isolated_kraft],
        num_failed_brokers=[1, 2]
    )
    @matrix(
        clean_shutdown=[True, False],
        metadata_quorum=[quorum.combined_kraft],
        num_failed_brokers=[1]
    )
    def test_broker_failure(self, clean_shutdown, metadata_quorum=quorum.isolated_kraft, num_failed_brokers=1):

        producer = self.setup_producer(self.TOPIC2["name"])
        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        producer.start()
        self.await_produced_messages(producer, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        # shutdown the required number of brokers
        self.fail_brokers(self.TOPIC2, num_brokers=num_failed_brokers, clean_shutdown=clean_shutdown, timeout_sec=self.default_timeout_sec)

        # ensure that the share consumers do some work after the broker failure
        self.await_consumed_messages(consumer, min_messages=1000, timeout_sec=self.default_timeout_sec)

        producer.stop()

        self.await_unique_consumed_messages(consumer, min_messages=producer.num_acked, timeout_sec=self.default_timeout_sec)

        assert consumer.total_unique_consumed() >= producer.num_acked

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        clean_shutdown=[True, False],
        bounce_mode=["all", "rolling"],
        metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft]
    )
    def test_share_consumer_bounce(self, clean_shutdown, bounce_mode, metadata_quorum=quorum.zk):
        """
        Verify correct share consumer behavior when the share consumers in the group are consecutively restarted.

        Setup: single Kafka cluster with one producer and a set of share consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the share consumers and wait until they've joined the group.
        - In a loop, restart each share consumer, waiting for each one to rejoin the group before
          restarting the rest.
        - Verify that the share consumers consume all messages produced by the producer atleast once.
        """

        producer = self.setup_producer(self.TOPIC2["name"])
        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        producer.start()
        self.await_produced_messages(producer, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        if bounce_mode == "all":
            self.bounce_all_share_consumers(consumer, clean_shutdown=clean_shutdown, timeout_sec=self.default_timeout_sec)
        else:
            self.rolling_bounce_share_consumers(consumer, clean_shutdown=clean_shutdown, timeout_sec=self.default_timeout_sec)

        producer.stop()

        self.await_unique_consumed_messages(consumer, min_messages=producer.num_acked, timeout_sec=self.default_timeout_sec)

        assert consumer.total_unique_consumed() >= producer.num_acked

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        clean_shutdown=[True, False],
        num_failed_consumers=[1, 2],
        metadata_quorum=[quorum.isolated_kraft, quorum.combined_kraft]
    )
    def test_share_consumer_failure(self, clean_shutdown, metadata_quorum=quorum.zk, num_failed_consumers=1):

        producer = self.setup_producer(self.TOPIC2["name"])
        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        # startup the producer and ensure that some records have been written
        producer.start()
        self.await_produced_messages(producer, timeout_sec=self.default_timeout_sec)

        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)

        # stop the required number of share consumers
        self.fail_share_consumers(consumer, num_failed_consumers, clean_shutdown=clean_shutdown, timeout_sec=self.default_timeout_sec)

        # ensure that the remaining consumer does some work
        self.await_consumed_messages(consumer, min_messages=1000, timeout_sec=self.default_timeout_sec)

        producer.stop()

        self.await_unique_consumed_messages(consumer, min_messages=producer.num_acked, timeout_sec=self.default_timeout_sec)

        assert consumer.total_unique_consumed() >= producer.num_acked

        consumer.stop_all()