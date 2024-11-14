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

from ducktape.mark import matrix, parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int_with_prefix
from kafkatest.version import DEV_BRANCH, \
    LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, LATEST_2_5, LATEST_2_6, LATEST_2_7, LATEST_2_8, \
    LATEST_3_0, LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, LATEST_3_6, LATEST_3_7, LATEST_3_8, LATEST_3_9, KafkaVersion

class ClientCompatibilityProduceConsumeTest(ProduceConsumeValidateTest):
    """
    These tests validate that we can use a new client to produce and consume from older brokers.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClientCompatibilityProduceConsumeTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=3) if quorum.for_test(test_context) == quorum.zk else None
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
        if self.zk:
            self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ClientCompatibilityProduceConsumeTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @cluster(num_nodes=9)
    @matrix(broker_version=[str(DEV_BRANCH)], metadata_quorum=quorum.all_non_upgrade)
    @parametrize(broker_version=str(LATEST_2_1))
    @parametrize(broker_version=str(LATEST_2_2))
    @parametrize(broker_version=str(LATEST_2_3))
    @parametrize(broker_version=str(LATEST_2_4))
    @parametrize(broker_version=str(LATEST_2_5))
    @parametrize(broker_version=str(LATEST_2_6))
    @parametrize(broker_version=str(LATEST_2_7))
    @parametrize(broker_version=str(LATEST_2_8))
    @parametrize(broker_version=str(LATEST_3_0))
    @parametrize(broker_version=str(LATEST_3_1))
    @parametrize(broker_version=str(LATEST_3_2))
    @parametrize(broker_version=str(LATEST_3_3))
    @parametrize(broker_version=str(LATEST_3_4))
    @parametrize(broker_version=str(LATEST_3_5))
    @parametrize(broker_version=str(LATEST_3_6))
    @parametrize(broker_version=str(LATEST_3_7))
    @parametrize(broker_version=str(LATEST_3_8))
    @parametrize(broker_version=str(LATEST_3_9))
    def test_produce_consume(self, broker_version, metadata_quorum=quorum.zk):
        print("running producer_consumer_compat with broker_version = %s" % broker_version, flush=True)
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

