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

class CompressionTest(ProduceConsumeValidateTest):
    """
    These tests validate produce / consume for compressed topics.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(CompressionTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk, topics={self.topic: {
                                                                    "partitions": 10,
                                                                    "replication-factor": 1}})
        self.num_partitions = 10
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 4
        self.messages_per_producer = 1000
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(CompressionTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @parametrize(compression_types=["snappy","gzip","lz4","none"], new_consumer=True)
    @parametrize(compression_types=["snappy","gzip","lz4","none"], new_consumer=False)
    def test_compressed_topic(self, compression_types, new_consumer):
        """Test produce => consume => validate for compressed topics
        Setup: 1 zk, 1 kafka node, 1 topic with partitions=10, replication-factor=1

        compression_types parameter gives a list of compression types (or no compression if
        "none"). Each producer in a VerifiableProducer group (num_producers = 4) will use a
        compression type from the list based on producer's index in the group.

            - Produce messages in the background
            - Consume messages in the background
            - Stop producing, and finish consuming
            - Validate that every acked message was consumed
        """

        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = self.kafka.security_protocol
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int_with_prefix,
                                           compression_types=compression_types)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic,
                                        new_consumer=new_consumer, consumer_timeout_ms=60000,
                                        message_validator=is_int_with_prefix)
        self.kafka.start()

        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))

