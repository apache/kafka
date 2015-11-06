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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer, is_int
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.mirror_maker import MirrorMaker
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest

import time


def bounce(test, clean_shutdown=True):
    """Bounce mirror maker with a clean (kill -15) or hard (kill -9) shutdown"""

    # Wait until messages start appearing in the target cluster
    wait_until(lambda: len(test.consumer.messages_consumed[1]) > 0, timeout_sec=15)

    # Wait for at least one offset to be committed
    time.sleep(test.mirror_maker.offset_commit_interval_ms / 1000.0 + .5)

    for i in range(1):
        test.logger.info("Bringing mirror maker nodes down...")
        for node in test.mirror_maker.nodes:
            test.mirror_maker.stop_node(node, clean_shutdown=clean_shutdown)

        num_consumed = len(test.consumer.messages_consumed[1])
        test.logger.info("Bringing mirror maker nodes back up...")
        for node in test.mirror_maker.nodes:
            test.mirror_maker.start_node(node)

        wait_until(lambda: len(test.consumer.messages_consumed[1]) > num_consumed + 100, timeout_sec=30)


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
        self.mirror_maker = MirrorMaker(test_context, num_nodes=1, source=self.source_kafka, target=self.target_kafka,
                                        whitelist=self.topic)
        # This will consume from target kafka cluster
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.target_kafka, topic=self.topic,
                                        message_validator=is_int, consumer_timeout_ms=30000)

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

    @parametrize(new_consumer=True)
    @parametrize(new_consumer=False)
    def test_simple_end_to_end(self, new_consumer):
        """
        Test end-to-end behavior under non-failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node mirror maker mirrors from source to target.

        - Start mirror maker.
        - Produce a small number of messages to the source cluster.
        - Consume messages from target.
        - Verify that number of consumed messages matches the number produced.
        """
        self.mirror_maker.new_consumer = new_consumer
        self.mirror_maker.start()

        mm_node = self.mirror_maker.nodes[0]
        with mm_node.account.monitor_log(self.mirror_maker.LOG_FILE) as monitor:
            if new_consumer:
                monitor.wait_until("Resetting offset for partition", timeout_sec=30, err_msg="Mirrormaker did not reset fetch offset in a reasonable amount of time.")
            else:
                monitor.wait_until("reset fetch offset", timeout_sec=30, err_msg="Mirrormaker did not reset fetch offset in a reasonable amount of time.")

        self.run_produce_consume_validate(core_test_action=self.wait_for_n_messages)
        self.mirror_maker.stop()

    @matrix(offsets_storage=["kafka", "zookeeper"], new_consumer=[False], clean_shutdown=[False])
    @matrix(new_consumer=[True], clean_shutdown=[False])
    def test_bounce(self, offsets_storage="kafka", new_consumer=True, clean_shutdown=True):
        """
        Test end-to-end behavior under failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node mirror maker mirrors from source to target.

        - Start mirror maker.
        - Produce to source cluster, and consume from target cluster in the background.
        - Bounce MM process
        - Verify every message acknowledged by the source producer is consumed by the target consumer
        """

        self.mirror_maker.offsets_storage = offsets_storage
        self.mirror_maker.new_consumer = new_consumer
        self.mirror_maker.start()

        # Wait until mirror maker has reset fetch offset at least once before continuing with the rest of the test
        mm_node = self.mirror_maker.nodes[0]
        with mm_node.account.monitor_log(self.mirror_maker.LOG_FILE) as monitor:
            if new_consumer:
                monitor.wait_until("Resetting offset for partition", timeout_sec=30, err_msg="Mirrormaker did not reset fetch offset in a reasonable amount of time.")
            else:
                monitor.wait_until("reset fetch offset", timeout_sec=30, err_msg="Mirrormaker did not reset fetch offset in a reasonable amount of time.")

        self.run_produce_consume_validate(core_test_action=lambda: bounce(self, clean_shutdown=clean_shutdown))
        self.mirror_maker.stop()
