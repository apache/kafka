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


from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import LATEST_0_8_2, TRUNK
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int
from kafkatest.services.kafka import config_property
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest


class TestUpgrade(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(TestUpgrade, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, version=LATEST_0_8_2, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}})
        self.zk.start()
        self.kafka.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput, version=LATEST_0_8_2)

        # TODO - reduce the timeout
        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=30000, message_validator=is_int, version=LATEST_0_8_2)

    def perform_upgrade(self):
        self.logger.info("First pass bounce - rolling upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = TRUNK
            node.config[config_property.INTER_BROKER_PROTOCOL_VERSION] = "0.8.2.X"
            self.kafka.start_node(node)

        self.logger.info("Second pass bounce - remove inter.broker.protocol.version config")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            del node.config[config_property.INTER_BROKER_PROTOCOL_VERSION]
            self.kafka.start_node(node)

    def test_upgrade(self):
        """Test upgrade of Kafka broker cluster from 0.8.2 to 0.9.0

        - Start 3 node broker cluster on version 0.8.2
        - Start producer and consumer in the background
        - Perform two-phase rolling upgrade
            - First phase: upgrade brokers to 0.9.0 with inter.broker.protocol.version set to 0.8.2.X
            - Second phase: remove inter.broker.protocol.version config with rolling bounce
        - Finally, validate that every message acked by the producer was consumed by the consumer
        """

        self.run_produce_consume_validate(core_test_action=self.perform_upgrade)


