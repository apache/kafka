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
import time

from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int_with_prefix
from kafkatest.fault.network_fracture import NetworkFracture


class PartitionedProduceConsumeTest(ProduceConsumeValidateTest):
    """
    Validate that we can continue to produce and consume in the presence of a network partition.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(PartitionedProduceConsumeTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics={self.topic:{
                                                                    "partitions": 10,
                                                                    "replication-factor": 3,
                                                                    "configs": {"min.insync.replicas": 3}}})
        self.num_partitions = 10
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 1
        self.messages_per_producer = 1000
        self.num_consumers = 1
        self.fracture = None

    def setUp(self):
        self.zk.start()

    def teardown(self):
        super(PartitionedProduceConsumeTest, self).teardown()
        if self.fracture is not None:
            self.fracture.stop()
            self.fracture = None

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(PartitionedProduceConsumeTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def test_produce_consume(self):
        self.logger.info("Starting test PartitionedProduceConsumeTest.")
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = self.kafka.security_protocol
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int_with_prefix, acks=-1, enable_idempotence=True)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int_with_prefix)
        self.kafka.start()

        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.produce_consume_validate_complete(),
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))

    def produce_consume_validate_complete(self):
        if ((self.fracture is None) and (self.producer.each_produced_at_least(100))):
            self.fracture = NetworkFracture(self.logger, [[self.kafka.nodes[0]], self.kafka.nodes[1:]])
            self.logger.info("Created %s" % str(self.fracture))
            self.fracture.start()
        return self.producer.each_produced_at_least(self.messages_per_producer)
