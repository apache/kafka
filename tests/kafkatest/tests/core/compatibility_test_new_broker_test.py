# Copyright 2015 Confluent Inc.
#
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

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import config_property
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_0_10_0, LATEST_0_9, LATEST_0_8_2, TRUNK, KafkaVersion


# Compatibility tests for moving to a new broker (e.g., 0.10.x) and using a mix of old and new clients (e.g., 0.9.x)
class ClientCompatibilityTestNewBroker(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(ClientCompatibilityTestNewBroker, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
            
        self.zk.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1
        self.messages_per_producer = 1000

    @parametrize(producer_version=str(LATEST_0_8_2), consumer_version=str(LATEST_0_8_2), compression_types=["none"], timestamp_type=None)
    @parametrize(producer_version=str(LATEST_0_8_2), consumer_version=str(LATEST_0_9), compression_types=["none"], timestamp_type=None)
    @parametrize(producer_version=str(LATEST_0_9), consumer_version=str(TRUNK), compression_types=["none"], timestamp_type=None)
    @parametrize(producer_version=str(TRUNK), consumer_version=str(LATEST_0_9), compression_types=["none"], timestamp_type=None)
    @parametrize(producer_version=str(LATEST_0_9), consumer_version=str(TRUNK), compression_types=["snappy"], new_consumer=True, timestamp_type=None)
    @parametrize(producer_version=str(TRUNK), consumer_version=str(LATEST_0_9), compression_types=["snappy"], new_consumer=True, timestamp_type=str("CreateTime"))
    @parametrize(producer_version=str(TRUNK), consumer_version=str(TRUNK), compression_types=["snappy"], new_consumer=True, timestamp_type=str("LogAppendTime"))
    @parametrize(producer_version=str(LATEST_0_10_0), consumer_version=str(LATEST_0_10_0), compression_types=["snappy"], new_consumer=True, timestamp_type=str("LogAppendTime"))
    @parametrize(producer_version=str(LATEST_0_9), consumer_version=str(LATEST_0_9), compression_types=["snappy"], new_consumer=True, timestamp_type=str("LogAppendTime"))
    @parametrize(producer_version=str(TRUNK), consumer_version=str(TRUNK), compression_types=["none"], timestamp_type=str("LogAppendTime"))
    def test_compatibility(self, producer_version, consumer_version, compression_types, new_consumer=False, timestamp_type=None):
       
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, version=TRUNK, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}})
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
                                        self.topic, consumer_timeout_ms=30000, new_consumer=new_consumer,
                                        message_validator=is_int, version=KafkaVersion(consumer_version))

        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))
