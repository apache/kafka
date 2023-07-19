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
from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.console_consumer import ConsoleConsumer

MAX_MESSAGES = 100
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

TOPIC_TEST_NAME = "topic-get-offset-shell-topic-name"

TOPIC_TEST_PATTERN_PREFIX = "topic-get-offset-shell-topic-pattern"
TOPIC_TEST_PATTERN_PATTERN = TOPIC_TEST_PATTERN_PREFIX + ".*"
TOPIC_TEST_PATTERN1 = TOPIC_TEST_PATTERN_PREFIX + "1"
TOPIC_TEST_PATTERN2 = TOPIC_TEST_PATTERN_PREFIX + "2"

TOPIC_TEST_PARTITIONS = "topic-get-offset-shell-partitions"

TOPIC_TEST_INTERNAL_FILTER = "topic-get-offset-shell-consumer_offsets"

TOPIC_TEST_TOPIC_PARTITIONS_PREFIX = "topic-get-offset-shell-topic-partitions"
TOPIC_TEST_TOPIC_PARTITIONS_PATTERN = TOPIC_TEST_TOPIC_PARTITIONS_PREFIX + ".*"
TOPIC_TEST_TOPIC_PARTITIONS1 = TOPIC_TEST_TOPIC_PARTITIONS_PREFIX + "1"
TOPIC_TEST_TOPIC_PARTITIONS2 = TOPIC_TEST_TOPIC_PARTITIONS_PREFIX + "2"


class GetOffsetShellTest(Test):
    """
    Tests GetOffsetShell tool
    """
    def __init__(self, test_context):
        super(GetOffsetShellTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.messages_received_count = 0
        self.topics = {
            TOPIC_TEST_NAME: {'partitions': NUM_PARTITIONS, 'replication-factor': REPLICATION_FACTOR},
            TOPIC_TEST_PATTERN1: {'partitions': 1, 'replication-factor': REPLICATION_FACTOR},
            TOPIC_TEST_PATTERN2: {'partitions': 1, 'replication-factor': REPLICATION_FACTOR},
            TOPIC_TEST_PARTITIONS: {'partitions': 2, 'replication-factor': REPLICATION_FACTOR},
            TOPIC_TEST_INTERNAL_FILTER: {'partitions': 1, 'replication-factor': REPLICATION_FACTOR},
            TOPIC_TEST_TOPIC_PARTITIONS1: {'partitions': 2, 'replication-factor': REPLICATION_FACTOR},
            TOPIC_TEST_TOPIC_PARTITIONS2: {'partitions': 2, 'replication-factor': REPLICATION_FACTOR}
        }

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

    def setUp(self):
        if self.zk:
            self.zk.start()

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics)
        self.kafka.start()

    def start_producer(self, topic):
        # This will produce to kafka cluster
        self.producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka, topic=topic,
                                           throughput=1000, max_messages=MAX_MESSAGES, repeating_keys=MAX_MESSAGES)
        self.producer.start()
        current_acked = self.producer.num_acked
        wait_until(lambda: self.producer.num_acked >= current_acked + MAX_MESSAGES, timeout_sec=10,
                   err_msg="Timeout awaiting messages to be produced and acked")

    def start_consumer(self, topic):
        self.consumer = ConsoleConsumer(self.test_context, num_nodes=self.num_brokers, kafka=self.kafka, topic=topic,
                                        consumer_timeout_ms=1000)
        self.consumer.start()

    def check_message_count_sum_equals(self, message_count, **kwargs):
        sum = self.extract_message_count_sum(**kwargs)
        return sum == message_count

    def extract_message_count_sum(self, **kwargs):
        offsets = self.kafka.get_offset_shell(**kwargs).split("\n")
        sum = 0
        for offset in offsets:
            if not offset:
                continue
            sum += int(offset.split(":")[-1])
        return sum

    @cluster(num_nodes=3)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_get_offset_shell_topic_name(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.zk):
        """
        Tests if GetOffsetShell handles --topic argument with a simple name correctly
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_producer(TOPIC_TEST_NAME)

        # Assert that offset is correctly indicated by GetOffsetShell tool
        wait_until(lambda: self.check_message_count_sum_equals(MAX_MESSAGES, topic=TOPIC_TEST_NAME),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

    @cluster(num_nodes=4)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_get_offset_shell_topic_pattern(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.zk):
        """
        Tests if GetOffsetShell handles --topic argument with a pattern correctly
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_producer(TOPIC_TEST_PATTERN1)
        self.start_producer(TOPIC_TEST_PATTERN2)

        # Assert that offset is correctly indicated by GetOffsetShell tool
        wait_until(lambda: self.check_message_count_sum_equals(2*MAX_MESSAGES, topic=TOPIC_TEST_PATTERN_PATTERN),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

    @cluster(num_nodes=3)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_get_offset_shell_partitions(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.zk):
        """
        Tests if GetOffsetShell handles --partitions argument correctly
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_producer(TOPIC_TEST_PARTITIONS)

        def fetch_and_sum_partitions_separately():
            partition_count0 = self.extract_message_count_sum(topic=TOPIC_TEST_PARTITIONS, partitions="0")
            partition_count1 = self.extract_message_count_sum(topic=TOPIC_TEST_PARTITIONS, partitions="1")
            return partition_count0 + partition_count1 == MAX_MESSAGES

        # Assert that offset is correctly indicated when fetching partitions one by one
        wait_until(fetch_and_sum_partitions_separately, timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

        # Assert that offset is correctly indicated when fetching partitions together
        wait_until(lambda: self.check_message_count_sum_equals(MAX_MESSAGES, topic=TOPIC_TEST_PARTITIONS),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

    @cluster(num_nodes=4)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_get_offset_shell_topic_partitions(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.zk):
        """
        Tests if GetOffsetShell handles --topic-partitions argument correctly
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_producer(TOPIC_TEST_TOPIC_PARTITIONS1)
        self.start_producer(TOPIC_TEST_TOPIC_PARTITIONS2)

        # Assert that a single topic pattern matches all 4 partitions
        wait_until(lambda: self.check_message_count_sum_equals(2*MAX_MESSAGES, topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS_PATTERN),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

        # Assert that a topic pattern with partition range matches all 4 partitions
        wait_until(lambda: self.check_message_count_sum_equals(2*MAX_MESSAGES, topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS_PATTERN + ":0-2"),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

        # Assert that 2 separate topic patterns match all 4 partitions
        wait_until(lambda: self.check_message_count_sum_equals(2*MAX_MESSAGES, topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS1 + "," + TOPIC_TEST_TOPIC_PARTITIONS2),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

        # Assert that 4 separate topic-partition patterns match all 4 partitions
        wait_until(lambda: self.check_message_count_sum_equals(2*MAX_MESSAGES,
                                                               topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS1 + ":0," +
                                                                                TOPIC_TEST_TOPIC_PARTITIONS1 + ":1," +
                                                                                TOPIC_TEST_TOPIC_PARTITIONS2 + ":0," +
                                                                                TOPIC_TEST_TOPIC_PARTITIONS2 + ":1"),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

        # Assert that only partitions #0 are matched with topic pattern and fix partition number
        filtered_partitions = self.kafka.get_offset_shell(topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS_PATTERN + ":0")
        assert 1 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS1, 0))
        assert 0 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS1, 1))
        assert 1 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS2, 0))
        assert 0 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS2, 1))

        # Assert that only partitions #1 are matched with topic pattern and partition lower bound
        filtered_partitions = self.kafka.get_offset_shell(topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS_PATTERN + ":1-")
        assert 1 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS1, 1))
        assert 0 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS1, 0))
        assert 1 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS2, 1))
        assert 0 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS2, 0))

        # Assert that only partitions #0 are matched with topic pattern and partition upper bound
        filtered_partitions = self.kafka.get_offset_shell(topic_partitions=TOPIC_TEST_TOPIC_PARTITIONS_PATTERN + ":-1")
        assert 1 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS1, 0))
        assert 0 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS1, 1))
        assert 1 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS2, 0))
        assert 0 == filtered_partitions.count("%s:%s" % (TOPIC_TEST_TOPIC_PARTITIONS2, 1))

    @cluster(num_nodes=4)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_get_offset_shell_internal_filter(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.zk):
        """
        Tests if GetOffsetShell handles --exclude-internal-topics flag correctly
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_producer(TOPIC_TEST_INTERNAL_FILTER)

        # Create consumer and poll messages to create consumer offset record
        self.start_consumer(TOPIC_TEST_INTERNAL_FILTER)
        node = self.consumer.nodes[0]
        wait_until(lambda: self.consumer.alive(node), timeout_sec=20, backoff_sec=.2, err_msg="Consumer was too slow to start")

        # Assert that a single topic pattern matches all 4 partitions
        wait_until(lambda: self.check_message_count_sum_equals(MAX_MESSAGES, topic_partitions=TOPIC_TEST_INTERNAL_FILTER),
                   timeout_sec=10, err_msg="Timed out waiting to reach expected offset.")

        # No filters
        # Assert that without exclusion, we can find both the test topic and the __consumer_offsets internal topic
        offset_output = self.kafka.get_offset_shell()
        assert "__consumer_offsets" in offset_output
        assert TOPIC_TEST_INTERNAL_FILTER in offset_output

        # Assert that with exclusion, we can find the test topic but not the __consumer_offsets internal topic
        offset_output = self.kafka.get_offset_shell(exclude_internal_topics=True)
        assert "__consumer_offsets" not in offset_output
        assert TOPIC_TEST_INTERNAL_FILTER in offset_output

        # Topic filter
        # Assert that without exclusion, we can find both the test topic and the __consumer_offsets internal topic
        offset_output = self.kafka.get_offset_shell(topic=".*consumer_offsets")
        assert "__consumer_offsets" in offset_output
        assert TOPIC_TEST_INTERNAL_FILTER in offset_output

        # Assert that with exclusion, we can find the test topic but not the __consumer_offsets internal topic
        offset_output = self.kafka.get_offset_shell(topic=".*consumer_offsets", exclude_internal_topics=True)
        assert "__consumer_offsets" not in offset_output
        assert TOPIC_TEST_INTERNAL_FILTER in offset_output

        # Topic-partition filter
        # Assert that without exclusion, we can find both the test topic and the __consumer_offsets internal topic
        offset_output = self.kafka.get_offset_shell(topic_partitions=".*consumer_offsets:0")
        assert "__consumer_offsets" in offset_output
        assert TOPIC_TEST_INTERNAL_FILTER in offset_output

        # Assert that with exclusion, we can find the test topic but not the __consumer_offsets internal topic
        offset_output = self.kafka.get_offset_shell(topic_partitions=".*consumer_offsets:0", exclude_internal_topics=True)
        assert "__consumer_offsets" not in offset_output
        assert TOPIC_TEST_INTERNAL_FILTER in offset_output
