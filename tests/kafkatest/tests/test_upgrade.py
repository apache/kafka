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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, KafkaVersion
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int
from kafkatest.services.kafka import kafka_prop


class UpgradeTest(Test):

    def __init__(self, test_context):
        super(UpgradeTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, version=KafkaVersion.V_0_8_2_1, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    "min.insync.replicas": 2}
                                                                })
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def test_upgrade(self):
        """Test upgrade of Kafka broker cluster from 0.8.2 to 0.9.0

        - Start 3 node broker cluster on version 0.8.2
        - Start producer and consumer in the background
        - Perform two-phase rolling upgrade
            - First phase: upgrade brokers to 0.9.0 with inter.broker.protocol.version set to 0.8.2
            - Second phase: remove inter.broker.protocol.version config with rolling bounce
        - Finally, validate that every message acked by the producer was consumed by the consumer
        """

        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput, version=KafkaVersion.V_0_8_2_1)
        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=10000, message_validator=is_int, version=KafkaVersion.V_0_8_2_1)

        # Start background producer and consumer
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=5,
             err_msg="Producer failed to start in a reasonable amount of time.")
        self.consumer.start()

        self.logger.info("First pass bounce - rolling upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = KafkaVersion.TRUNK
            node.config[kafka_prop.INTER_BROKER_PROTOCOL_VERSION] = "0.8.2.X"
            self.kafka.start_node(node)

        self.logger.info("Second pass bounce - remove inter.broker.protocol.version config")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            del node.config[kafka_prop.INTER_BROKER_PROTOCOL_VERSION]
            self.kafka.start_node(node)

        self.producer.stop()
        for node in self.consumer.nodes:
            assert self.consumer.alive(node)
        self.consumer.wait()

        self.acked = self.producer.acked
        self.not_acked = self.producer.not_acked

        # Check produced vs consumed
        self.consumed = self.consumer.messages_consumed[1]
        self.logger.info("num consumed:  %d" % len(self.consumed))
        success, msg = self.validate()

        if not success:
            self.mark_for_collect(self.producer)

        assert success, msg

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

