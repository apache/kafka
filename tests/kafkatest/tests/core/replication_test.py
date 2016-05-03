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
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

import signal

def broker_node(test, broker_type):
    """ Discover node of requested type. For leader type, discovers leader for our topic and partition 0
    """
    if broker_type == "leader":
        node = test.kafka.leader(test.topic, partition=0)
    elif broker_type == "controller":
        node = test.kafka.controller()
    else:
        raise Exception("Unexpected broker type %s." % (broker_type))

    return node

def clean_shutdown(test, broker_type):
    """Discover broker node of requested type and shut it down cleanly.
    """
    node = broker_node(test, broker_type)
    test.kafka.signal_node(node, sig=signal.SIGTERM)


def hard_shutdown(test, broker_type):
    """Discover broker node of requested type and shut it down with a hard kill."""
    node = broker_node(test, broker_type)
    test.kafka.signal_node(node, sig=signal.SIGKILL)


def clean_bounce(test, broker_type):
    """Chase the leader of one partition and restart it cleanly."""
    for i in range(5):
        prev_broker_node = broker_node(test, broker_type)
        test.kafka.restart_node(prev_broker_node, clean_shutdown=True)


def hard_bounce(test, broker_type):
    """Chase the leader and restart it with a hard kill."""
    for i in range(5):
        prev_broker_node = broker_node(test, broker_type)
        test.kafka.signal_node(prev_broker_node, sig=signal.SIGKILL)

        # Since this is a hard kill, we need to make sure the process is down and that
        # zookeeper and the broker cluster have registered the loss of the leader/controller.
        # Waiting for a new leader for the topic-partition/controller to be elected is a reasonable heuristic for this.

        def role_reassigned():
            current_elected_broker = broker_node(test, broker_type)
            return current_elected_broker is not None and current_elected_broker != prev_broker_node

        wait_until(lambda: len(test.kafka.pids(prev_broker_node)) == 0, timeout_sec=5)
        wait_until(role_reassigned, timeout_sec=10, backoff_sec=.5)
        test.kafka.start_node(prev_broker_node)

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
            broker_type=["leader"],
            security_protocol=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["controller"],
            security_protocol=["PLAINTEXT", "SASL_SSL"])
    @matrix(failure_mode=["hard_bounce"],
            broker_type=["leader"],
            security_protocol=["SASL_SSL"], client_sasl_mechanism=["PLAIN"], interbroker_sasl_mechanism=["PLAIN", "GSSAPI"])
    def test_replication_with_broker_failure(self, failure_mode, security_protocol, broker_type, client_sasl_mechanism="GSSAPI", interbroker_sasl_mechanism="GSSAPI"):
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

        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.client_sasl_mechanism = client_sasl_mechanism
        self.kafka.interbroker_sasl_mechanism = interbroker_sasl_mechanism
        new_consumer = False if  self.kafka.security_protocol == "PLAINTEXT" else True
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, throughput=self.producer_throughput)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic, new_consumer=new_consumer, consumer_timeout_ms=60000, message_validator=is_int)
        self.kafka.start()
        
        self.run_produce_consume_validate(core_test_action=lambda: failures[failure_mode](self, broker_type))
