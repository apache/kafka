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

import os
import shlex

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService, config_property, quorum
from kafkatest.services.kafka.kafka import KafkaListener
from kafkatest.services.security.security_config import SecurityConfig


class MultipleListenersTest(Test):
    """Tests clusters with multiple listener names using the same security protocol."""

    PERSISTENT_ROOT = "/mnt/multiple_listeners"
    COMMAND_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "client.properties")
    EXTERNAL_LISTENER = "EXTERNAL"
    TOPIC_PREFIX = "multiple-listeners"

    def __init__(self, test_context):
        super(MultipleListenersTest, self).__init__(test_context)
        self.num_brokers = 1
        self.listener_names = [SecurityConfig.PLAINTEXT, self.EXTERNAL_LISTENER]
        self.topics = {
            self.topic_name(listener_name): {'partitions': 1, 'replication-factor': 1}
            for listener_name in self.listener_names
        }

    def topic_name(self, listener_name):
        return "%s-%s" % (self.TOPIC_PREFIX, listener_name.lower())

    def start_kafka(self):
        self.kafka = KafkaService(
            self.test_context,
            self.num_brokers,
            None,
            security_protocol=SecurityConfig.PLAINTEXT,
            interbroker_security_protocol=SecurityConfig.PLAINTEXT,
            topics=self.topics,
            controller_num_nodes_override=self.num_brokers)
        self.kafka.port_mappings[self.EXTERNAL_LISTENER] = KafkaListener(
            self.EXTERNAL_LISTENER,
            config_property.FIRST_BROKER_PORT + 8,
            SecurityConfig.PLAINTEXT,
            True)
        self.kafka.start()

    def client_config(self, node):
        node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)
        node.account.create_file(self.COMMAND_CONFIG_FILE, "security.protocol=PLAINTEXT\n")

    def produce_and_consume(self, listener_name):
        node = self.kafka.nodes[0]
        bootstrap_servers = self.kafka.bootstrap_servers(listener_name)
        topic = self.topic_name(listener_name)
        message = "message-from-%s" % listener_name.lower()
        console_producer = self.kafka.path.script("kafka-console-producer.sh", node)
        console_consumer = self.kafka.path.script("kafka-console-consumer.sh", node)

        produce_cmd = "printf '%%s\\n' %s | %s --bootstrap-server %s --topic %s --command-config %s" % (
            shlex.quote(message),
            console_producer,
            shlex.quote(bootstrap_servers),
            shlex.quote(topic),
            self.COMMAND_CONFIG_FILE)
        node.account.ssh(produce_cmd, allow_fail=False)

        consume_cmd = "%s --bootstrap-server %s --topic %s --from-beginning --max-messages 1 " \
                      "--timeout-ms 10000 --command-config %s" % (
                          console_consumer,
                          shlex.quote(bootstrap_servers),
                          shlex.quote(topic),
                          self.COMMAND_CONFIG_FILE)
        output = "\n".join(line.strip() for line in node.account.ssh_capture(consume_cmd, allow_fail=False))
        assert message in output, "Could not consume expected message from listener %s. Output: %s" % (
            listener_name,
            output)

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_multiple_listeners_same_security_protocol(self, metadata_quorum=quorum.isolated_kraft):
        self.start_kafka()
        self.client_config(self.kafka.nodes[0])

        for listener_name in self.listener_names:
            self.produce_and_consume(listener_name)
