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
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster

from kafkatest.tests.end_to_end import EndToEndTest

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
        # zookeeper has registered the loss by expiring the broker's session timeout.

        wait_until(lambda: len(test.kafka.pids(prev_broker_node)) == 0 and not test.kafka.is_registered(prev_broker_node),
                   timeout_sec=test.kafka.zk_session_timeout + 5,
                   err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(prev_broker_node.account))

        test.kafka.start_node(prev_broker_node)

failures = {
    "clean_shutdown": clean_shutdown,
    "hard_shutdown": hard_shutdown,
    "clean_bounce": clean_bounce,
    "hard_bounce": hard_bounce
}


class ReplicationTest(EndToEndTest):
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

    TOPIC_CONFIG = {
        "partitions": 3,
        "replication-factor": 3,
        "configs": {"min.insync.replicas": 2}
    }
 
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReplicationTest, self).__init__(test_context=test_context, topic_config=self.TOPIC_CONFIG)
 
    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(ReplicationTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader"],
            security_protocol=["PLAINTEXT"],
            enable_idempotence=[True])
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader"],
            security_protocol=["PLAINTEXT", "SASL_SSL"])
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["controller"],
            security_protocol=["PLAINTEXT", "SASL_SSL"])
    @matrix(failure_mode=["hard_bounce"],
            broker_type=["leader"],
            security_protocol=["SASL_SSL"], client_sasl_mechanism=["PLAIN"], interbroker_sasl_mechanism=["PLAIN", "GSSAPI"])
    @parametrize(failure_mode="hard_bounce",
            broker_type="leader",
            security_protocol="SASL_SSL", client_sasl_mechanism="SCRAM-SHA-256", interbroker_sasl_mechanism="SCRAM-SHA-512")
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            security_protocol=["PLAINTEXT"], broker_type=["leader"], compression_type=["gzip"])
    def test_replication_with_broker_failure(self, failure_mode, security_protocol, broker_type,
                                             client_sasl_mechanism="GSSAPI", interbroker_sasl_mechanism="GSSAPI",
                                             compression_type=None, enable_idempotence=False):
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

        self.create_zookeeper()
        self.zk.start()

        self.create_kafka(num_nodes=3,
                          security_protocol=security_protocol,
                          interbroker_security_protocol=security_protocol,
                          client_sasl_mechanism=client_sasl_mechanism,
                          interbroker_sasl_mechanism=interbroker_sasl_mechanism)
        self.kafka.start()

        compression_types = None if not compression_type else [compression_type]
        self.create_producer(compression_types=compression_types, enable_idempotence=enable_idempotence)
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()

        self.await_startup()
        failures[failure_mode](self, broker_type)
        self.run_validation(enable_idempotence=enable_idempotence)
