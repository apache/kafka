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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import LATEST_0_8_2, TRUNK
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer, is_int


class ClientCompatibilityTest(Test):

    def __init__(self, test_context):
        super(ClientCompatibilityTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, version=LATEST_0_8_2, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    "min.insync.replicas": 2}})
        self.zk.start()
        self.kafka.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1

    def test_producer_back_compatibility(self):
        """Run 0.9.X java producer against 0.8.X brokers.
        This test documents the fact that java producer v0.9.0.0 and later won't run against 0.8.X brokers
        the broker responds to a V1 produce request with a V0 fetch response; the client then tries to parse this V0
        produce response as a V1 produce response, resulting in a BufferUnderflowException
        """
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic, max_messages=100,
            throughput=self.producer_throughput, version=TRUNK)

        node = self.producer.nodes[0]
        try:
            self.producer.start()
            self.producer.wait()
            raise Exception("0.9.X java producer should not run successfully against 0.8.X broker")
        except:
            # Expected
            pass
        finally:
            self.producer.kill_node(node, clean_shutdown=False)

        self.logger.info("Grepping producer log for expected error type")
        node.account.ssh("egrep -m 1 %s %s" % ("\"org\.apache\.kafka\.common\.protocol\.types\.SchemaException.*throttle_time_ms.*: java\.nio\.BufferUnderflowException\"", self.producer.LOG_FILE), allow_fail=False)

    def test_consumer_back_compatibility(self):
        """Run the scala 0.8.X consumer against an 0.9.X cluster.
        Expect 0.8.X scala consumer to fail with buffer underflow. This error is the same as when an 0.9.X producer
        is run against an 0.8.X broker: the broker responds to a V1 fetch request with a V0 fetch response; the
        client then tries to parse this V0 fetch response as a V1 fetch response, resulting in a BufferUnderflowException
        """
        num_messages = 10
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic, max_messages=num_messages,
            throughput=self.producer_throughput, version=LATEST_0_8_2)

        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic, group_id="consumer-09X",
            consumer_timeout_ms=10000, message_validator=is_int, version=TRUNK)

        self.old_consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic, group_id="consumer-08X",
            consumer_timeout_ms=10000, message_validator=is_int, version=LATEST_0_8_2)

        self.producer.run()
        self.consumer.run()
        self.old_consumer.run()

        consumed = len(self.consumer.messages_consumed[1])
        old_consumed = len(self.old_consumer.messages_consumed[1])
        assert old_consumed == num_messages, "Expected 0.8.X scala consumer to consume %d, but only got %d" % (num_messages, old_consumed)
        assert consumed == 0, "Expected 0.9.X scala consumer to fail to consume any messages, but got %d" % consumed

        self.logger.info("Grepping consumer log for expected error type")
        node = self.consumer.nodes[0]
        node.account.ssh("egrep -m 1 %s %s" % ("\"java\.nio\.BufferUnderflowException\"", self.consumer.LOG_FILE), allow_fail=False)



