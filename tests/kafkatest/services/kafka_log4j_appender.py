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

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.security.security_config import SecurityConfig


class KafkaLog4jAppender(KafkaPathResolverMixin, BackgroundThreadService):

    logs = {
        "producer_log": {
            "path": "/mnt/kafka_log4j_appender.log",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, max_messages=-1, security_protocol="PLAINTEXT"):
        super(KafkaLog4jAppender, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.max_messages = max_messages
        self.security_protocol = security_protocol
        self.security_config = SecurityConfig(security_protocol)

    def _worker(self, idx, node):
        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableLog4jAppender %d command: %s" % (idx, cmd))
        self.security_config.setup_node(node)
        node.account.ssh(cmd)

    def start_cmd(self, node):
        cmd = self.path.script("kafka-run-class.sh", node)
        cmd += " org.apache.kafka.tools.VerifiableLog4jAppender"
        cmd += " --topic %s --broker-list %s" % (self.topic, self.kafka.bootstrap_servers(self.security_protocol))

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        if self.security_protocol != SecurityConfig.PLAINTEXT:
            cmd += " --security-protocol %s" % str(self.security_protocol)
        if self.security_protocol == SecurityConfig.SSL or self.security_protocol == SecurityConfig.SASL_SSL:
            cmd += " --ssl-truststore-location %s" % str(SecurityConfig.TRUSTSTORE_PATH)
            cmd += " --ssl-truststore-password %s" % str(SecurityConfig.ssl_stores.truststore_passwd)
        if self.security_protocol == SecurityConfig.SASL_PLAINTEXT or \
                self.security_protocol == SecurityConfig.SASL_SSL or \
                self.security_protocol == SecurityConfig.SASL_MECHANISM_GSSAPI or \
                self.security_protocol == SecurityConfig.SASL_MECHANISM_PLAIN:
            cmd += " --sasl-kerberos-service-name %s" % str('kafka')
            cmd += " --client-jaas-conf-path %s" % str(SecurityConfig.JAAS_CONF_PATH)
            cmd += " --kerb5-conf-path %s" % str(SecurityConfig.KRB5CONF_PATH)

        cmd += " 2>> /mnt/kafka_log4j_appender.log | tee -a /mnt/kafka_log4j_appender.log &"
        return cmd

    def stop_node(self, node):
        node.account.kill_process("VerifiableLog4jAppender", allow_fail=False)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        node.account.kill_process("VerifiableLog4jAppender", clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf /mnt/kafka_log4j_appender.log", allow_fail=False)
