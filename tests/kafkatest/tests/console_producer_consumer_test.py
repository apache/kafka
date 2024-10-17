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
from kafkatest.services.console_producer import ConsoleProducer

class ConsoleProducerConsumerTest(Test):

    TOPIC = "test-topic"

    def __init__(self, test_context):
        super(ConsoleProducerConsumerTest, self).__init__(test_context=test_context)
        self.zk = ZookeeperService(self.test_context, 1)
        self.kafka = KafkaService(self.test_context, 1, self.zk)
        self.consumer = ConsoleConsumer(self.test_context, 1, self.kafka, self.TOPIC)
        self.producer = ConsoleProducer(self.test_context, 1, self.kafka, self.TOPIC)

    def setUp(self):
        self.zk.start()
        self.kafka.start()
        topic_cfg = {
            "topic": self.TOPIC,
            "partitions": 1,
            "replication-factor": 1
        }
        self.kafka.create_topic(topic_cfg)
        self.consumer.start()
        self.producer.start()

    def test_produce_consume(self):
        self.producer.produce(self.producer.nodes[0], ["hi", "hello", "bye"])
        wait_until(lambda: len(self.consumer.messages_consumed[1]) == 3,
                timeout_sec=30,
                backoff_sec=5, err_msg="waiting for messages")
