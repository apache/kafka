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

from ducktape.tests.test import Test
from ducktape.mark import parametrize
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import LATEST_0_9, LATEST_0_10, TRUNK, KafkaVersion
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.utils import is_int
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.services.kafka import config_property
import time

#
# This tests performs the following checks:
# 1. Have both 0.9 and 0.10 producer and consumer produce to and consume from a 0.10.x cluster
# 2. initially the topic is using message format 0.9.0
# 3. change the message format version for topic to 0.10.0 on the fly.
# 4. change the message format version for topic back to 0.9.0 on the fly.
# The producers and consumers should not have any issue.
# 
class MessageFormatChangeTest(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(MessageFormatChangeTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
            
        self.zk.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1

    def change_format(self, topic):
        self.logger.info("First format change to 0.9.0")
        self.kafka.alter_message_format(topic, str(LATEST_0_9))
        time.sleep(1)
        self.logger.info("Second format change to 0.10.0")
        self.kafka.alter_message_format(topic, str(LATEST_0_10))
        time.sleep(1)
        self.logger.info("Third format change back to 0.9.0")
        self.kafka.alter_message_format(topic, str(LATEST_0_9))
        time.sleep(1)
        
    @parametrize(producer_version=str(TRUNK), consumer_version=str(TRUNK))
    def test_compatibility(self, producer_version, consumer_version):
       
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, version=TRUNK, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}})
       
        self.kafka.start()
         
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           version=KafkaVersion(producer_version))

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=KafkaVersion(consumer_version))

        self.run_produce_consume_validate(core_test_action=lambda: self.change_format(self.topic))


