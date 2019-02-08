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
from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig

import os
import re

TOPIC = "topic-consumer-group-command"


class ConsumerGroupCommandTest(Test):
    """
    Tests ConsumerGroupCommand
    """
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/consumer_group_command"
    COMMAND_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "command.properties")

    def __init__(self, test_context):
        super(ConsumerGroupCommandTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 1}
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

    def start_consumer(self):
        self.consumer = ConsoleConsumer(self.test_context, num_nodes=self.num_brokers, kafka=self.kafka, topic=TOPIC,
                                        consumer_timeout_ms=None)
        self.consumer.start()

    def setup_and_verify(self, security_protocol, group=None):
        self.start_kafka(security_protocol, security_protocol)
        self.start_consumer()
        consumer_node = self.consumer.nodes[0]
        wait_until(lambda: self.consumer.alive(consumer_node),
                   timeout_sec=10, backoff_sec=.2, err_msg="Consumer was too slow to start")
        kafka_node = self.kafka.nodes[0]
        if security_protocol is not SecurityConfig.PLAINTEXT:
            prop_file = str(self.kafka.security_config.client_config())
            self.logger.debug(prop_file)
            kafka_node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)
            kafka_node.account.create_file(self.COMMAND_CONFIG_FILE, prop_file)

        # Verify ConsumerGroupCommand lists expected consumer groups
        command_config_file = self.COMMAND_CONFIG_FILE

        if group:
            wait_until(lambda: re.search("topic-consumer-group-command",self.kafka.describe_consumer_group(group=group, node=kafka_node, command_config=command_config_file)), timeout_sec=10,
                       err_msg="Timed out waiting to list expected consumer groups.")
        else:
            wait_until(lambda: "test-consumer-group" in self.kafka.list_consumer_groups(node=kafka_node, command_config=command_config_file), timeout_sec=10,
                       err_msg="Timed out waiting to list expected consumer groups.")

        self.consumer.stop()

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_list_consumer_groups(self, security_protocol='PLAINTEXT'):
        """
        Tests if ConsumerGroupCommand is listing correct consumer groups
        :return: None
        """
        self.setup_and_verify(security_protocol)

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_describe_consumer_group(self, security_protocol='PLAINTEXT'):
        """
        Tests if ConsumerGroupCommand is describing a consumer group correctly
        :return: None
        """
        self.setup_and_verify(security_protocol, group="test-consumer-group")
