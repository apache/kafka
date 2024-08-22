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
from ducktape.mark import parametrize, matrix
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.connect_mirror_maker import ConnectMirrorMaker
from kafkatest.services.security.minikdc import MiniKdc
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import V_2_3_0

import time


class TestConnectMirrorMakerService(ProduceConsumeValidateTest):
    """Sanity checks on MirrorMaker 2.0 service class."""
    def __init__(self, test_context):
        super(TestConnectMirrorMakerService, self).__init__(test_context)

        self.producer_start_timeout_sec = 100
        self.consumer_start_timeout_sec = 100

        self.topic = "FOO"
        self.source_zk = ZookeeperService(test_context, num_nodes=1)
        self.target_zk = ZookeeperService(test_context, num_nodes=1)

        self.source_kafka = KafkaService(test_context, num_nodes=1, zk=self.source_zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.target_kafka = KafkaService(test_context, num_nodes=1, zk=self.target_zk)

        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(test_context, num_nodes=1, kafka=self.source_kafka, topic=self.topic,
                                           throughput=1000)

        self.mm2 = ConnectMirrorMaker(test_context, num_nodes=1, source=self.source_kafka, target=self.target_kafka,
                                        topics=self.topic)

        # This will consume from target kafka cluster
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.target_kafka, topic="source."+self.topic,
                                        message_validator=is_int, consumer_timeout_ms=60000)

    def setUp(self):
        # Source cluster
        self.source_zk.start()

        # Target cluster
        self.target_zk.start()

    def start_kafka(self, security_protocol):
        self.source_kafka.security_protocol = security_protocol
        self.source_kafka.interbroker_security_protocol = security_protocol
        self.target_kafka.security_protocol = security_protocol
        self.target_kafka.interbroker_security_protocol = security_protocol
        if self.source_kafka.security_config.has_sasl_kerberos:
            minikdc = MiniKdc(self.source_kafka.context, self.source_kafka.nodes + self.target_kafka.nodes)
            self.source_kafka.minikdc = minikdc
            self.target_kafka.minikdc = minikdc
            minikdc.start()
        self.source_kafka.start()
        self.target_kafka.start()

    def bounce(self, clean_shutdown):
        """Bounce MM2 with a clean (kill -15) or hard (kill -9) shutdown"""

        # Wait until messages start appearing in the target cluster
        wait_until(lambda: len(self.consumer.messages_consumed[1]) > 0, timeout_sec=15)

        for i in range(3):
            self.logger.info("Bringing MirrorMaker 2.0 nodes down...")
            for node in self.mm2.nodes:
                self.mm2.stop_node(node, clean_shutdown=clean_shutdown)

            num_consumed = len(self.consumer.messages_consumed[1])
            self.logger.info("Bringing MirrorMaker 2.0 nodes back up...")
            for node in self.mm2.nodes:
                self.mm2.start_node(node)

            # Ensure new messages are once again showing up on the target cluster
            wait_until(lambda: len(self.consumer.messages_consumed[1]) > num_consumed + 10, timeout_sec=60)

    def wait_for_n_messages(self, n_messages=10):
        """Wait for a minimum number of messages to be successfully produced."""
        wait_until(lambda: self.producer.num_acked > n_messages, timeout_sec=10,
                     err_msg="Producer failed to produce %d messages in a reasonable amount of time." % n_messages)

    @cluster(num_nodes=7)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    @cluster(num_nodes=8)
    @matrix(security_protocol=['SASL_PLAINTEXT', 'SASL_SSL'])
    def test_simple_end_to_end(self, security_protocol):
        """
        Test end-to-end behavior under non-failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node MM2 replicates from source to target.

        - Start MM2.
        - Produce a small number of messages to the source cluster.
        - Consume messages from target.
        - Verify that number of consumed messages matches the number produced.
        """
        self.start_kafka(security_protocol)
        self.mm2.start()

        self.run_produce_consume_validate(core_test_action=self.wait_for_n_messages)
        self.mm2.stop()

    @cluster(num_nodes=7)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    @cluster(num_nodes=8)
    @matrix(security_protocol=['SASL_PLAINTEXT', 'SASL_SSL'])
    def test_bounce(self, offsets_storage="kafka", security_protocol='PLAINTEXT'):
        """
        Test end-to-end behavior under failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node MM2 replicates from source to target.

        - Start MM2.
        - Produce to source cluster, and consume from target cluster in the background.
        - Bounce MM2 process
        - Verify every message acknowledged by the source producer is consumed by the target consumer
        """
        self.start_kafka(security_protocol)
        self.mm2.start()

        self.run_produce_consume_validate(core_test_action=lambda: self.bounce(clean_shutdown=true))
        self.mm2.stop()
