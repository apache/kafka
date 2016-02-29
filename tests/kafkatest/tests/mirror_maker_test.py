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
from ducktape.mark import parametrize, matrix, ignore

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.mirror_maker import MirrorMaker
from kafkatest.services.security.minikdc import MiniKdc
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

import time


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
                                        whitelist=self.topic, offset_commit_interval_ms=1000)
        # This will consume from target kafka cluster
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.target_kafka, topic=self.topic,
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

    def bounce(self, clean_shutdown=True):
        """Bounce mirror maker with a clean (kill -15) or hard (kill -9) shutdown"""

        # Wait until messages start appearing in the target cluster
        wait_until(lambda: len(self.consumer.messages_consumed[1]) > 0, timeout_sec=15)

        # Wait for at least one offset to be committed.
        #
        # This step is necessary to prevent data loss with default mirror maker settings:
        # currently, if we don't have at least one committed offset,
        # and we bounce mirror maker, the consumer internals will throw OffsetOutOfRangeException, and the default
        # auto.offset.reset policy ("largest") will kick in, causing mirrormaker to start consuming from the largest
        # offset. As a result, any messages produced to the source cluster while mirrormaker was dead won't get
        # mirrored to the target cluster.
        # (see https://issues.apache.org/jira/browse/KAFKA-2759)
        #
        # This isn't necessary with kill -15 because mirror maker commits its offsets during graceful
        # shutdown.
        if not clean_shutdown:
            time.sleep(self.mirror_maker.offset_commit_interval_ms / 1000.0 + .5)

        for i in range(3):
            self.logger.info("Bringing mirror maker nodes down...")
            for node in self.mirror_maker.nodes:
                self.mirror_maker.stop_node(node, clean_shutdown=clean_shutdown)

            num_consumed = len(self.consumer.messages_consumed[1])
            self.logger.info("Bringing mirror maker nodes back up...")
            for node in self.mirror_maker.nodes:
                self.mirror_maker.start_node(node)

            # Ensure new messages are once again showing up on the target cluster
            # new consumer requires higher timeout here
            wait_until(lambda: len(self.consumer.messages_consumed[1]) > num_consumed + 100, timeout_sec=60)

    def wait_for_n_messages(self, n_messages=100):
        """Wait for a minimum number of messages to be successfully produced."""
        wait_until(lambda: self.producer.num_acked > n_messages, timeout_sec=10,
                     err_msg="Producer failed to produce %d messages in a reasonable amount of time." % n_messages)

    @parametrize(security_protocol='PLAINTEXT', new_consumer=False)
    @matrix(security_protocol=['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'], new_consumer=[True])
    def test_simple_end_to_end(self, security_protocol, new_consumer):
        """
        Test end-to-end behavior under non-failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node mirror maker mirrors from source to target.

        - Start mirror maker.
        - Produce a small number of messages to the source cluster.
        - Consume messages from target.
        - Verify that number of consumed messages matches the number produced.
        """
        self.start_kafka(security_protocol)
        self.consumer.new_consumer = new_consumer

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

    @matrix(offsets_storage=["kafka", "zookeeper"], new_consumer=[False], clean_shutdown=[True, False])
    @matrix(new_consumer=[True], clean_shutdown=[True, False], security_protocol=['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'])
    def test_bounce(self, offsets_storage="kafka", new_consumer=True, clean_shutdown=True, security_protocol='PLAINTEXT'):
        """
        Test end-to-end behavior under failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node mirror maker mirrors from source to target.

        - Start mirror maker.
        - Produce to source cluster, and consume from target cluster in the background.
        - Bounce MM process
        - Verify every message acknowledged by the source producer is consumed by the target consumer
        """
        if new_consumer and not clean_shutdown:
            # Increase timeout on downstream console consumer; mirror maker with new consumer takes extra time
            # during hard bounce. This is because the restarted mirror maker consumer won't be able to rejoin
            # the group until the previous session times out
            self.consumer.consumer_timeout_ms = 60000

        self.start_kafka(security_protocol)
        self.consumer.new_consumer = new_consumer

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

        self.run_produce_consume_validate(core_test_action=lambda: self.bounce(clean_shutdown=clean_shutdown))
        self.mirror_maker.stop()
