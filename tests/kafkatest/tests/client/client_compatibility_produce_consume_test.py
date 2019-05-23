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
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int_with_prefix
from kafkatest.version import DEV_BRANCH, LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, KafkaVersion

class ClientCompatibilityProduceConsumeTest(ProduceConsumeValidateTest):
    """
    These tests validate that we can use a new client to produce and consume from older brokers.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClientCompatibilityProduceConsumeTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics={self.topic:{
                                                                    "partitions": 10,
                                                                    "replication-factor": 2}})
        self.num_partitions = 10
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 2
        self.messages_per_producer = 1000
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ClientCompatibilityProduceConsumeTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @parametrize(broker_version=str(DEV_BRANCH))
    @parametrize(broker_version=str(LATEST_0_10_0))
    @parametrize(broker_version=str(LATEST_0_10_1))
    @parametrize(broker_version=str(LATEST_0_10_2))
    @parametrize(broker_version=str(LATEST_0_11_0))
    @parametrize(broker_version=str(LATEST_1_0))
    @parametrize(broker_version=str(LATEST_1_1))
    @parametrize(broker_version=str(LATEST_2_0))
    @parametrize(broker_version=str(LATEST_2_1))
    @parametrize(broker_version=str(LATEST_2_2))
    def test_produce_consume(self, broker_version):
        print("running producer_consumer_compat with broker_version = %s" % broker_version)
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = self.kafka.security_protocol
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int_with_prefix)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int_with_prefix)
        self.kafka.start()

        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))

