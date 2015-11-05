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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.mirror_maker import MirrorMaker
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest


def clean_bounce(test):
    for i in range(3):
        test.logger.info("Bringing mirror maker nodes down...")
        for node in test.mirror_maker.nodes:
            test.mirror_maker.stop_node(node)

        test.logger.info("Bringing mirror maker nodes back up...")
        for node in test.mirror_maker.nodes:
            test.mirror_maker.start_node(node)


def hard_bounce(test):
    for i in range(1):

        test.logger.info("Bringing mirror maker nodes down...")
        for node in test.mirror_maker.nodes:
            test.mirror_maker.stop_node(node, clean_shutdown=False)

        test.logger.info("Bringing mirror maker nodes back up...")
        for node in test.mirror_maker.nodes:
            test.mirror_maker.start_node(node)


class TestMirrorMakerService(ProduceConsumeValidateTest):
    """Sanity checks on mirror maker service class."""
    def __init__(self, test_context):
        super(TestMirrorMakerService, self).__init__(test_context)

        self.topic = "topic"
        self.source_zk = ZookeeperService(test_context, num_nodes=1)
        self.target_zk = ZookeeperService(test_context, num_nodes=1)

        self.source_kafka = KafkaService(test_context, num_nodes=1, zk=self.source_zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.target_kafka = KafkaService(test_context, num_nodes=1, zk=self.target_zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})

        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(test_context, num_nodes=1, kafka=self.source_kafka, topic=self.topic,
                                           throughput=1000)

        # Use a regex whitelist to check that the start command is well-formed in this case
        self.mirror_maker = MirrorMaker(test_context, num_nodes=1, source=self.source_kafka, target=self.target_kafka,
                                        whitelist=".*", consumer_timeout_ms=30000)

        # This will consume from target kafka cluster
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.target_kafka, topic=self.topic,
                                        consumer_timeout_ms=180000)

    def setUp(self):
        # Source cluster
        self.source_zk.start()
        self.source_kafka.start()

        # Target cluster
        self.target_zk.start()
        self.target_kafka.start()

    def wait_for_n_messages(self, n_messages=100):
        """Wait for a minimum number of messages to be successfully produced."""
        wait_until(lambda: self.producer.num_acked > n_messages, timeout_sec=10,
                     err_msg="Producer failed to produce %d messages in a reasonable amount of time." % n_messages)

    def test_end_to_end(self):
        """
        Test end-to-end behavior under non-failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node mirror maker mirrors from source to target.

        - Start mirror maker.
        - Produce a small number of messages to the source cluster.
        - Consume messages from target.
        - Verify that number of consumed messages matches the number produced.
        """
        self.mirror_maker.start()
        self.run_produce_consume_validate(core_test_action=self.wait_for_n_messages)
        self.mirror_maker.stop()

    def test_clean_bounce(self):
        self.mirror_maker.start()
        self.run_produce_consume_validate(core_test_action=lambda: clean_bounce(self))
        self.mirror_maker.stop()

    def test_hard_bounce(self):
        self.mirror_maker.start()
        self.run_produce_consume_validate(core_test_action=lambda: hard_bounce(self))
        self.mirror_maker.stop()
