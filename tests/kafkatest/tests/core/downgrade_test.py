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
from ducktape.mark.resource import cluster

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import config_property
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_0_8_2, LATEST_0_9, LATEST_0_10, LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, V_0_9_0_0, V_0_11_0_0, DEV_BRANCH, KafkaVersion

class TestDowngrade(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(TestDowngrade, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # Producer and consumer
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def upgrade_from(self, kafka_version):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = DEV_BRANCH
            node.config[config_property.INTER_BROKER_PROTOCOL_VERSION] = kafka_version
            node.config[config_property.MESSAGE_FORMAT_VERSION] = kafka_version
            self.kafka.start_node(node)

    def downgrade_to(self, kafka_version):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = KafkaVersion(kafka_version)
            del node.config[config_property.INTER_BROKER_PROTOCOL_VERSION]
            del node.config[config_property.MESSAGE_FORMAT_VERSION]
            self.kafka.start_node(node)

    @cluster(num_nodes=7)
    @parametrize(kafka_version=str(LATEST_2_3), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_2_3), compression_types=["zstd"])
    @parametrize(kafka_version=str(LATEST_2_2), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_2_2), compression_types=["zstd"])
    @parametrize(kafka_version=str(LATEST_2_1), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_2_1), compression_types=["lz4"])
    @parametrize(kafka_version=str(LATEST_2_0), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_2_0), compression_types=["snappy"])
    @parametrize(kafka_version=str(LATEST_1_1), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_1_1), compression_types=["lz4"])
    @parametrize(kafka_version=str(LATEST_1_0), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_1_0), compression_types=["snappy"])
    @parametrize(kafka_version=str(LATEST_0_11_0), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_0_11_0), compression_types=["lz4"])
    @parametrize(kafka_version=str(LATEST_0_10_2), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_0_10_2), compression_types=["lz4"])
    @parametrize(kafka_version=str(LATEST_0_10_1), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_0_10_1), compression_types=["gzip"])
    @parametrize(kafka_version=str(LATEST_0_10_0), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_0_10_0), compression_types=["lz4"])
    @parametrize(kafka_version=str(LATEST_0_9), compression_types=["none"], security_protocol="SASL_SSL")
    @parametrize(kafka_version=str(LATEST_0_9), compression_types=["snappy"])
    @parametrize(kafka_version=str(LATEST_0_9), compression_types=["lz4"])
    @parametrize(kafka_version=str(LATEST_0_8_2), compression_types=["none"])
    @parametrize(kafka_version=str(LATEST_0_8_2), compression_types=["snappy"])
    def test_upgrade_and_downgrade(self, kafka_version, compression_types, security_protocol="PLAINTEXT"):
        """Test upgrade and downgrade of Kafka cluster from old versions to the current version

        kafka_version is a Kafka version to downgrade to

        Downgrades are supported to any version which is at or above the current 
        `inter.broker.protocol.version` (IBP). For example, if a user upgrades from 1.1 to 2.3, 
        but they leave the IBP set to 1.1, then downgrading to any version at 1.1 or higher is 
        supported.

        This test case verifies that producers and consumers continue working during
        the course of an upgrade and downgrade.

        - Start 3 node broker cluster on version 'kafka_version'
        - Start producer and consumer in the background
        - Roll the cluster to upgrade to the current version with IBP set to 'kafka_version'
        - Roll the cluster to downgrade back to 'kafka_version'
        - Finally, validate that every message acked by the producer was consumed by the consumer
        """
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk,
                                  version=KafkaVersion(kafka_version),
                                  topics={self.topic: {"partitions": 3, "replication-factor": 3,
                                                       'configs': {"min.insync.replicas": 2}}})
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.start()

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=compression_types,
                                           version=KafkaVersion(kafka_version))

        if kafka_version <= LATEST_0_10_0:
            assert self.kafka.cluster_id() is None

        # With older message formats before KIP-101, message loss may occur due to truncation
        # after leader change. Tolerate limited data loss for this case to avoid transient test failures.
        self.may_truncate_acked_records = False if kafka_version >= V_0_11_0_0 else True

        new_consumer = kafka_version >= V_0_9_0_0

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, new_consumer=new_consumer, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=KafkaVersion(kafka_version))

        def upgrade_and_downgrade():
            self.logger.info("First pass bounce - rolling upgrade")
            self.upgrade_from(kafka_version)
            yield
            
            self.logger.info("Second pass bounce - rolling downgrade")
            self.downgrade_to(kafka_version)
            yield

        self.run_multistep_produce_consume_validate(upgrade_and_downgrade)
