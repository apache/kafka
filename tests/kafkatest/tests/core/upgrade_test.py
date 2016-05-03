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

from ducktape.mark import parametrize
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import LATEST_0_8_2, LATEST_0_9, TRUNK, KafkaVersion
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import config_property
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int


class TestUpgrade(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(TestUpgrade, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1

    def perform_upgrade(self, from_kafka_version, to_message_format_version=None):
        self.logger.info("First pass bounce - rolling upgrade")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = TRUNK
            node.config[config_property.INTER_BROKER_PROTOCOL_VERSION] = from_kafka_version
            node.config[config_property.MESSAGE_FORMAT_VERSION] = from_kafka_version
            self.kafka.start_node(node)

        self.logger.info("Second pass bounce - remove inter.broker.protocol.version config")
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            del node.config[config_property.INTER_BROKER_PROTOCOL_VERSION]
            if to_message_format_version is None:
                del node.config[config_property.MESSAGE_FORMAT_VERSION]
            else:
                node.config[config_property.MESSAGE_FORMAT_VERSION] = to_message_format_version
            self.kafka.start_node(node)


    @parametrize(from_kafka_version=str(LATEST_0_9), to_message_format_version=None, compression_types=["none"])
    @parametrize(from_kafka_version=str(LATEST_0_9), to_message_format_version=None, compression_types=["snappy"], new_consumer=True)
    @parametrize(from_kafka_version=str(LATEST_0_9), to_message_format_version=str(LATEST_0_9), compression_types=["none"])
    @parametrize(from_kafka_version=str(LATEST_0_9), to_message_format_version=str(LATEST_0_9), compression_types=["snappy"], new_consumer=True)
    @parametrize(from_kafka_version=str(LATEST_0_8_2), to_message_format_version=None, compression_types=["none"])
    @parametrize(from_kafka_version=str(LATEST_0_8_2), to_message_format_version=None, compression_types=["snappy"])
    def test_upgrade(self, from_kafka_version, to_message_format_version, compression_types, new_consumer=False):
        """Test upgrade of Kafka broker cluster from 0.8.2 or 0.9.0 to 0.10

        from_kafka_version is a Kafka version to upgrade from: either 0.8.2.X or 0.9

        If to_message_format_version is None, it means that we will upgrade to default (latest)
        message format version. It is possible to upgrade to 0.10 brokers but still use message
        format version 0.9

        - Start 3 node broker cluster on version 'from_kafka_version'
        - Start producer and consumer in the background
        - Perform two-phase rolling upgrade
            - First phase: upgrade brokers to 0.10 with inter.broker.protocol.version set to
            from_kafka_version and log.message.format.version set to from_kafka_version
            - Second phase: remove inter.broker.protocol.version config with rolling bounce; if
            to_message_format_version is set to 0.9, set log.message.format.version to
            to_message_format_version, otherwise remove log.message.format.version config
        - Finally, validate that every message acked by the producer was consumed by the consumer
        """
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk,
                                  version=KafkaVersion(from_kafka_version),
                                  topics={self.topic: {"partitions": 3, "replication-factor": 3,
                                                       'configs': {"min.insync.replicas": 2}}})
        self.kafka.start()

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=compression_types,
                                           version=KafkaVersion(from_kafka_version))

        # TODO - reduce the timeout
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000, new_consumer=new_consumer,
                                        message_validator=is_int, version=KafkaVersion(from_kafka_version))

        self.run_produce_consume_validate(core_test_action=lambda: self.perform_upgrade(from_kafka_version,
                                                                                        to_message_format_version))
