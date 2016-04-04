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
from kafkatest.services.verifiable_producer import VerifiableProducer

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig

TOPIC = "topic-get-offset-shell"
MAX_MESSAGES = 100
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

class GetOffsetShellTest(Test):
    """
    Tests GetOffsetShell tool
    """
    def __init__(self, test_context):
        super(GetOffsetShellTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.messages_received_count = 0
        self.topics = {
            TOPIC: {'partitions': NUM_PARTITIONS, 'replication-factor': REPLICATION_FACTOR}
        }

        self.zk = ZookeeperService(test_context, self.num_zk)



    def setUp(self):
        self.zk.start()

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics)
        self.kafka.start()

    def start_producer(self):
        # This will produce to kafka cluster
        self.producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka, topic=TOPIC, throughput=1000, max_messages=MAX_MESSAGES)
        self.producer.start()
        current_acked = self.producer.num_acked
        wait_until(lambda: self.producer.num_acked >= current_acked + MAX_MESSAGES, timeout_sec=10,
                   err_msg="Timeout awaiting messages to be produced and acked")

    def start_consumer(self, security_protocol):
        enable_new_consumer = security_protocol != SecurityConfig.PLAINTEXT
        self.consumer = ConsoleConsumer(self.test_context, num_nodes=self.num_brokers, kafka=self.kafka, topic=TOPIC,
                                        consumer_timeout_ms=1000, new_consumer=enable_new_consumer)
        self.consumer.start()

    def test_get_offset_shell(self, security_protocol='PLAINTEXT'):
        """
        Tests if GetOffsetShell is getting offsets correctly
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_producer()

        # Assert that offset fetched without any consumers consuming is 0
        assert self.kafka.get_offset_shell(TOPIC, None, 1000, 1, -1), "%s:%s:%s" % (TOPIC, NUM_PARTITIONS - 1, 0)

        self.start_consumer(security_protocol)

        node = self.consumer.nodes[0]

        wait_until(lambda: self.consumer.alive(node), timeout_sec=10, backoff_sec=.2, err_msg="Consumer was too slow to start")

        # Assert that offset is correctly indicated by GetOffsetShell tool
        wait_until(lambda: "%s:%s:%s" % (TOPIC, NUM_PARTITIONS - 1, MAX_MESSAGES) in self.kafka.get_offset_shell(TOPIC, None, 1000, 1, -1), timeout_sec=10,
                   err_msg="Timed out waiting to reach expected offset.")