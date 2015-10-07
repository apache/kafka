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

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from ducktape.mark import matrix

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int

import signal
import time


class ReplicationTest(Test):
    """Replication tests.
    These tests verify that replication provides simple durability guarantees by checking that data acked by
    brokers is still available for consumption in the face of various failure scenarios."""

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReplicationTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(ReplicationTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def run_with_failure(self, failure, interbroker_security_protocol):
        """This is the top-level test template.

        The steps are:
            Produce messages in the background while driving some failure condition
            When done driving failures, immediately stop producing
            Consume all messages
            Validate that messages acked by brokers were consumed

        Note that consuming is a bit tricky, at least with console consumer. The goal is to consume all messages
        (foreach partition) in the topic. In this case, waiting for the last message may cause the consumer to stop
        too soon since console consumer is consuming multiple partitions from a single thread and therefore we lose
        ordering guarantees.

        Waiting on a count of consumed messages can be unreliable: if we stop consuming when num_consumed == num_acked,
        we might exit early if some messages are duplicated (though not an issue here since producer retries==0)

        Therefore rely here on the consumer.timeout.ms setting which times out on the interval between successively
        consumed messages. Since we run the producer to completion before running the consumer, this is a reliable
        indicator that nothing is left to consume.

        """
        security_protocol='PLAINTEXT'
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, 
                                  security_protocol=security_protocol,
                                  interbroker_security_protocol=interbroker_security_protocol,
                                  topics={self.topic: {
                                               "partitions": 3,
                                               "replication-factor": 3,
                                               "min.insync.replicas": 2}
                                         })
        self.kafka.start()
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, security_protocol=security_protocol, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, security_protocol=security_protocol, new_consumer=False, consumer_timeout_ms=3000, message_validator=is_int)

        # Produce in a background thread while driving broker failures
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=5,
             err_msg="Producer failed to start in a reasonable amount of time.")
        failure()
        self.producer.stop()

        self.acked = self.producer.acked
        self.not_acked = self.producer.not_acked
        self.logger.info("num not acked: %d" % self.producer.num_not_acked)
        self.logger.info("num acked:     %d" % self.producer.num_acked)

        # Consume all messages
        self.consumer.start()
        self.consumer.wait()
        self.consumed = self.consumer.messages_consumed[1]
        self.logger.info("num consumed:  %d" % len(self.consumed))

        # Check produced vs consumed
        success, msg = self.validate()

        if not success:
            self.mark_for_collect(self.producer)

        assert success, msg

    def clean_shutdown(self):
        """Discover leader node for our topic and shut it down cleanly."""
        self.kafka.signal_leader(self.topic, partition=0, sig=signal.SIGTERM)

    def hard_shutdown(self):
        """Discover leader node for our topic and shut it down with a hard kill."""
        self.kafka.signal_leader(self.topic, partition=0, sig=signal.SIGKILL)

    def clean_bounce(self):
        """Chase the leader of one partition and restart it cleanly."""
        for i in range(5):
            prev_leader_node = self.kafka.leader(topic=self.topic, partition=0)
            self.kafka.restart_node(prev_leader_node, wait_sec=5, clean_shutdown=True)

    def hard_bounce(self):
        """Chase the leader and restart it cleanly."""
        for i in range(5):
            prev_leader_node = self.kafka.leader(topic=self.topic, partition=0)
            self.kafka.restart_node(prev_leader_node, wait_sec=5, clean_shutdown=False)

            # Wait long enough for previous leader to probably be awake again
            time.sleep(6)

    def validate(self):
        """Check that produced messages were consumed."""

        success = True
        msg = ""

        if len(set(self.consumed)) != len(self.consumed):
            # There are duplicates. This is ok, so report it but don't fail the test
            msg += "There are duplicate messages in the log\n"

        if not set(self.consumed).issuperset(set(self.acked)):
            # Every acked message must appear in the logs. I.e. consumed messages must be superset of acked messages.
            acked_minus_consumed = set(self.producer.acked) - set(self.consumed)
            success = False
            msg += "At least one acked message did not appear in the consumed messages. acked_minus_consumed: " + str(acked_minus_consumed)

        if not success:
            # Collect all the data logs if there was a failure
            self.mark_for_collect(self.kafka)

        return success, msg

    
    @matrix(interbroker_security_protocol=['PLAINTEXT', 'SSL'])
    def test_clean_shutdown(self, interbroker_security_protocol):
        self.run_with_failure(self.clean_shutdown, interbroker_security_protocol)

    @matrix(interbroker_security_protocol=['PLAINTEXT', 'SSL'])
    def test_hard_shutdown(self, interbroker_security_protocol):
        self.run_with_failure(self.hard_shutdown, interbroker_security_protocol)

    @matrix(interbroker_security_protocol=['PLAINTEXT', 'SSL'])
    def test_clean_bounce(self, interbroker_security_protocol):
        self.run_with_failure(self.clean_bounce, interbroker_security_protocol)

    @matrix(interbroker_security_protocol=['PLAINTEXT', 'SSL'])
    def test_hard_bounce(self, interbroker_security_protocol):
        self.run_with_failure(self.hard_bounce, interbroker_security_protocol)
