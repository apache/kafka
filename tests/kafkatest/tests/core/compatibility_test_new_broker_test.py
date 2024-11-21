# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import matrix, parametrize
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.kafka import config_property
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, \
    LATEST_2_7, LATEST_2_8, LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, \
    LATEST_3_7, LATEST_3_8, LATEST_3_9, DEV_BRANCH, KafkaVersion

# Compatibility tests for moving to a new broker (e.g., 0.10.x) and using a mix of old and new clients (e.g., 0.9.x)
class ClientCompatibilityTestNewBroker(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(ClientCompatibilityTestNewBroker, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1
        self.messages_per_producer = 1000

    @cluster(num_nodes=6)
    @matrix(producer_version=[str(DEV_BRANCH)], consumer_version=[str(DEV_BRANCH)], compression_types=[["snappy"]], timestamp_type=[str("LogAppendTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(DEV_BRANCH)], consumer_version=[str(DEV_BRANCH)], compression_types=[["none"]], timestamp_type=[str("LogAppendTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(DEV_BRANCH)], consumer_version=[str(LATEST_2_1)], compression_types=[["snappy"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_2)], consumer_version=[str(LATEST_2_2)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_3)], consumer_version=[str(LATEST_2_3)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_4)], consumer_version=[str(LATEST_2_4)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_5)], consumer_version=[str(LATEST_2_5)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_6)], consumer_version=[str(LATEST_2_6)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_7)], consumer_version=[str(LATEST_2_7)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_8)], consumer_version=[str(LATEST_2_8)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_0)], consumer_version=[str(LATEST_3_0)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_1)], consumer_version=[str(LATEST_3_1)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_2)], consumer_version=[str(LATEST_3_2)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_3)], consumer_version=[str(LATEST_3_3)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_4)], consumer_version=[str(LATEST_3_4)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_5)], consumer_version=[str(LATEST_3_5)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_6)], consumer_version=[str(LATEST_3_6)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_7)], consumer_version=[str(LATEST_3_7)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_8)], consumer_version=[str(LATEST_3_8)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_3_9)], consumer_version=[str(LATEST_3_9)], compression_types=[["none"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    @matrix(producer_version=[str(LATEST_2_1)], consumer_version=[str(LATEST_2_1)], compression_types=[["zstd"]], timestamp_type=[str("CreateTime")], metadata_quorum=quorum.all_non_upgrade)
    def test_compatibility(self, producer_version, consumer_version, compression_types, timestamp_type=None, metadata_quorum=quorum.zk):
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=None, version=DEV_BRANCH, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}},
                                  controller_num_nodes_override=1)
        for node in self.kafka.nodes:
            if timestamp_type is not None:
                node.config[config_property.MESSAGE_TIMESTAMP_TYPE] = timestamp_type
        self.kafka.start()

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=compression_types,
                                           version=KafkaVersion(producer_version))

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=KafkaVersion(consumer_version))

        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))
