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


from ducktape.tests.test import Test

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.mirror_maker import MirrorMaker


class TestMirrorMakerService(Test):
    """Sanity checks on mirror maker service class."""
    def __init__(self, test_context):
        super(TestMirrorMakerService, self).__init__(test_context)

        self.topic = "topic"
        self.source_zk = ZookeeperService(test_context, num_nodes=1)
        self.target_zk = ZookeeperService(test_context, num_nodes=1)

        self.source_kafka = KafkaService(test_context, num_nodes=1, zk=self.source_zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.target_kafka = KafkaService(test_context, num_nodes=1, zk=self.target_zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})

        self.num_messages = 1000
        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(test_context, num_nodes=1, kafka=self.source_kafka, topic=self.topic,
                                           max_messages=self.num_messages, throughput=1000)

        # Use a regex whitelist to check that the start command is well-formed in this case
        self.mirror_maker = MirrorMaker(test_context, num_nodes=1, source=self.source_kafka, target=self.target_kafka,
                                        whitelist=".*", consumer_timeout_ms=2000)

        # This will consume from target kafka cluster
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.target_kafka, topic=self.topic,
                                        consumer_timeout_ms=1000)

    def setUp(self):
        # Source cluster
        self.source_zk.start()
        self.source_kafka.start()

        # Target cluster
        self.target_zk.start()
        self.target_kafka.start()

    def test_end_to_end(self):
        """
        Test end-to-end behavior under non-failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target. Single-node mirror maker mirrors from source to target.

        - Start mirror maker.
        - Produce a small number of messages to the source cluster.
        - Consume messages from target.
        - Verify that number of consumed messages matches the number produced.
        """
        self.mirror_maker.start()
        # Check that consumer_timeout_ms setting made it to config file
        self.mirror_maker.nodes[0].account.ssh(
            "grep \"consumer\.timeout\.ms\" %s" % MirrorMaker.CONSUMER_CONFIG, allow_fail=False)

        self.producer.start()
        self.producer.wait(10)
        self.consumer.start()
        self.consumer.wait(10)

        num_consumed = len(self.consumer.messages_consumed[1])
        num_produced = self.producer.num_acked
        assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)
        assert num_produced == num_consumed, "num_produced: %d, num_consumed: %d" % (num_produced, num_consumed)

        self.mirror_maker.stop()

