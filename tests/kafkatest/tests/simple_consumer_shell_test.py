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


from ducktape.utils.util import wait_until
from ducktape.tests.test import Test
from kafkatest.services.simple_consumer_shell import SimpleConsumerShell
from kafkatest.services.verifiable_producer import VerifiableProducer

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
TOPIC = "topic-simple-consumer-shell"
MAX_MESSAGES = 100
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

class SimpleConsumerShellTest(Test):
    """
    Tests SimpleConsumerShell tool
    """
    def __init__(self, test_context):
        super(SimpleConsumerShellTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.messages_received_count = 0
        self.topics = {
            TOPIC: {'partitions': NUM_PARTITIONS, 'replication-factor': REPLICATION_FACTOR}
        }

        self.zk = ZookeeperService(test_context, self.num_zk)

    def setUp(self):
        self.zk.start()

    def start_kafka(self):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, topics=self.topics)
        self.kafka.start()

    def run_producer(self):
        # This will produce to kafka cluster
        self.producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka, topic=TOPIC, throughput=1000, max_messages=MAX_MESSAGES)
        self.producer.start()
        wait_until(lambda: self.producer.num_acked == MAX_MESSAGES, timeout_sec=10,
                   err_msg="Timeout awaiting messages to be produced and acked")

    def start_simple_consumer_shell(self):
        self.simple_consumer_shell = SimpleConsumerShell(self.test_context, 1, self.kafka, TOPIC)
        self.simple_consumer_shell.start()

    def test_simple_consumer_shell(self):
        """
        Tests if SimpleConsumerShell is fetching expected records
        :return: None
        """
        self.start_kafka()
        self.run_producer()
        self.start_simple_consumer_shell()

        # Assert that SimpleConsumerShell is fetching expected number of messages
        wait_until(lambda: self.simple_consumer_shell.get_output().count("\n") == (MAX_MESSAGES + 1), timeout_sec=10,
                   err_msg="Timed out waiting to receive expected number of messages.")