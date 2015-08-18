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
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.mirror_maker import MirrorMaker


class TestMirrorMakerService(Test):
    """Sanity checks on console consumer service class."""
    def __init__(self, test_context):
        super(TestMirrorMakerService, self).__init__(test_context)

        self.topic = "topic"
        self.zk1 = ZookeeperService(test_context, num_nodes=1)
        self.zk2 = ZookeeperService(test_context, num_nodes=1)

        self.k1 = KafkaService(test_context, num_nodes=1, zk=self.zk1,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})
        self.k2 = KafkaService(test_context, num_nodes=1, zk=self.zk2,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})

        self.num_messages = 1000
        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(test_context, num_nodes=1, kafka=self.k1, topic=self.topic,
                                           max_messages=self.num_messages, throughput=1000)
        self.mirror_maker = MirrorMaker(test_context, sources=[self.k1], target=self.k2, whitelist=self.topic)

        # This will consume from target kafka cluster
        self.consumer = ConsoleConsumer(test_context, num_nodes=1, kafka=self.k2, topic=self.topic,
                                        consumer_timeout_ms=10000)

    def setUp(self):
        # Source cluster
        self.zk1.start()
        self.k1.start()

        # Target cluster
        self.zk2.start()
        self.k2.start()

    def test_lifecycle(self):
        """Start and stop a single-node MirrorMaker and validate that the process appears and disappears in a
        reasonable amount of time.
        """
        self.mirror_maker.start()
        node = self.mirror_maker.nodes[0]
        wait_until(lambda: self.mirror_maker.alive(node), timeout_sec=10, backoff_sec=.5,
                   err_msg="Mirror maker took too long to start.")

        self.mirror_maker.stop()
        wait_until(lambda: not self.mirror_maker.alive(node), timeout_sec=10, backoff_sec=.5,
                   err_msg="Mirror maker took to long to stop.")

    def test_end_to_end(self):
        """
        Test end-to-end behavior under non-failure conditions.

        Setup: two single node Kafka clusters, each connected to its own single node zookeeper cluster.
        One is source, and the other is target.

        - Start mirror maker.
        - Produce a small number of messages to the source cluster.
        - Consume messages from target.
        - Confirm that number of consumed messages matches the number produced.
        """
        self.mirror_maker.start()
        node = self.mirror_maker.nodes[0]
        wait_until(lambda: self.mirror_maker.alive(node), timeout_sec=10, backoff_sec=.5,
                   err_msg="Mirror maker took too long to start.")

        self.producer.start()
        self.producer.wait()
        self.consumer.start()
        self.consumer.wait()
        assert len(self.consumer.messages_consumed[1]) == self.num_messages

        self.mirror_maker.stop()
        wait_until(lambda: not self.mirror_maker.alive(node), timeout_sec=10, backoff_sec=.5,
                   err_msg="Mirror maker took to long to stop.")
