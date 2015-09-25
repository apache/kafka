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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest

import signal


def clean_shutdown(obj):
    """Discover leader node for our topic and shut it down cleanly."""
    obj.kafka.signal_leader(obj.topic, partition=0, sig=signal.SIGTERM)


def hard_shutdown(obj):
    """Discover leader node for our topic and shut it down with a hard kill."""
    obj.kafka.signal_leader(obj.topic, partition=0, sig=signal.SIGKILL)


def clean_bounce(obj):
    """Chase the leader of one partition and restart it cleanly."""
    for i in range(5):
        prev_leader_node = obj.kafka.leader(topic=obj.topic, partition=0)
        obj.kafka.restart_node(prev_leader_node, clean_shutdown=True)


def hard_bounce(obj):
    """Chase the leader and restart it cleanly."""
    for i in range(5):
        prev_leader_node = obj.kafka.leader(topic=obj.topic, partition=0)
        obj.kafka.restart_node(prev_leader_node, clean_shutdown=False)


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
                                                                    "min.insync.replicas": 2}
                                                                })
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(ReplicationTest, self).min_cluster_size() + self.num_producers + self.num_consumers


    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            interbroker_security_protocol=["PLAINTEXT", "SSL"])
    def test_replication_with_broker_failure(self, failure_mode, interbroker_security_protocol):
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
        client_security_protocol = 'PLAINTEXT'
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, security_protocol=client_security_protocol, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, security_protocol=client_security_protocol, consumer_timeout_ms=10000, message_validator=is_int)

        self.kafka.interbroker_security_protocol = interbroker_security_protocol
        self.kafka.start()
        
        self.run_default_test_template(core_test_action=lambda: failures[failure_mode](self))
