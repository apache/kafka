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

from ducktape.mark import matrix

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest

import signal


def clean_shutdown(test):
    """Discover leader node for our topic and shut it down cleanly."""
    test.kafka.signal_leader(test.topic, partition=0, sig=signal.SIGTERM)


def hard_shutdown(test):
    """Discover leader node for our topic and shut it down with a hard kill."""
    test.kafka.signal_leader(test.topic, partition=0, sig=signal.SIGKILL)


def clean_bounce(test):
    """Chase the leader of one partition and restart it cleanly."""
    for i in range(5):
        prev_leader_node = test.kafka.leader(topic=test.topic, partition=0)
        test.kafka.restart_node(prev_leader_node, clean_shutdown=True)


def hard_bounce(test):
    """Chase the leader and restart it with a hard kill."""
    for i in range(5):
        prev_leader_node = test.kafka.leader(topic=test.topic, partition=0)
        test.kafka.signal_node(prev_leader_node, sig=signal.SIGKILL)

        # Since this is a hard kill, we need to make sure the process is down and that
        # zookeeper and the broker cluster have registered the loss of the leader.
        # Waiting for a new leader to be elected on the topic-partition is a reasonable heuristic for this.

        def leader_changed():
            current_leader = test.kafka.leader(topic=test.topic, partition=0)
            return current_leader is not None and current_leader != prev_leader_node

        wait_until(lambda: len(test.kafka.pids(prev_leader_node)) == 0, timeout_sec=5)
        wait_until(leader_changed, timeout_sec=10, backoff_sec=.5)
        test.kafka.start_node(prev_leader_node)

failures = {
    "clean_shutdown": clean_shutdown,
    "hard_shutdown": hard_shutdown,
    "clean_bounce": clean_bounce,
    "hard_bounce": hard_bounce
}


class ReplicationTest(ProduceConsumeValidateTest):
    """
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

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReplicationTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}
                                                                    })
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(ReplicationTest, self).min_cluster_size() + self.num_producers + self.num_consumers


    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            security_protocol=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
    def test_replication_with_broker_failure(self, failure_mode, security_protocol):
        """Replication tests.
        These tests verify that replication provides simple durability guarantees by checking that data acked by
        brokers is still available for consumption in the face of various failure scenarios.

        Setup: 1 zk, 3 kafka nodes, 1 topic with partitions=3, replication-factor=3, and min.insync.replicas=2

            - Produce messages in the background
            - Consume messages in the background
            - Drive broker failures (shutdown, or bounce repeatedly with kill -15 or kill -9)
            - When done driving failures, stop producing, and finish consuming
            - Validate that every acked message was consumed
        """

        self.kafka.security_protocol = 'PLAINTEXT'
        self.kafka.interbroker_security_protocol = security_protocol
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, consumer_timeout_ms=60000, message_validator=is_int)
        self.kafka.start()
        
        self.run_produce_consume_validate(core_test_action=lambda: failures[failure_mode](self))
