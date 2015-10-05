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
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import TRUNK, LATEST_0_8_2
from kafkatest.utils.security_config import SecurityConfig


import json
import os


class VerifiableProducer(BackgroundThreadService):
    PERSISTENT_ROOT = "/mnt/verifiable_producer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_producer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_producer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_producer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "verifiable_producer.properties")

    logs = {
        "verifiable_producer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "verifiable_producer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "verifiable_producer_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, kafka, topic, security_protocol=None, max_messages=-1, throughput=100000, version=TRUNK):
        super(VerifiableProducer, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.max_messages = max_messages
        self.throughput = throughput

        for node in self.nodes:
            node.version = version
        self.acked_values = []
        self.not_acked_values = []

        self.prop_file = ""
        self.security_config = SecurityConfig(security_protocol, self.prop_file)
        self.security_protocol = self.security_config.security_protocol
        self.prop_file += str(self.security_config)

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % VerifiableProducer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=VerifiableProducer.LOG_FILE)
        node.account.create_file(VerifiableProducer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        self.logger.info("verifiable_producer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableProducer.CONFIG_FILE, self.prop_file)
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableProducer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            line = line.strip()

            data = self.try_parse_json(line)
            if data is not None:

                with self.lock:
                    if data["name"] == "producer_send_error":
                        data["node"] = idx
                        self.not_acked_values.append(int(data["value"]))

                    elif data["name"] == "producer_send_success":
                        self.acked_values.append(int(data["value"]))

    def start_cmd(self, node):

        cmd = ""
        if node.version <= LATEST_0_8_2:
            # 0.8.2.X releases do not have VerifiableProducer.java, so cheat and add
            # the tools jar from trunk to the classpath
            cmd += "for file in /opt/%s/tools/build/libs/kafka-tools*.jar; do CLASSPATH=$CLASSPATH:$file; done; " % KafkaService.KAFKA_TRUNK
            cmd += "for file in /opt/%s/tools/build/dependant-libs-${SCALA_VERSION}*/*.jar; do CLASSPATH=$CLASSPATH:$file; done; " % KafkaService.KAFKA_TRUNK
            cmd += "export CLASSPATH; "

        cmd += "export LOG_DIR=%s;" % VerifiableProducer.LOG_DIR
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % VerifiableProducer.LOG4J_CONFIG

        kafka_dir = "/opt/" + KafkaService.kafka_dir(node)
        cmd += kafka_dir + "/bin/kafka-run-class.sh org.apache.kafka.clients.tools.VerifiableProducer" \
              " --topic %s --broker-list %s" % (self.topic, self.kafka.bootstrap_servers())
        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        if self.throughput > 0:
            cmd += " --throughput %s" % str(self.throughput)

        cmd += " --producer.config %s" % VerifiableProducer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableProducer.STDOUT_CAPTURE, VerifiableProducer.STDOUT_CAPTURE)
        return cmd

    @property
    def acked(self):
        with self.lock:
            return self.acked_values

    @property
    def not_acked(self):
        with self.lock:
            return self.not_acked_values

    @property
    def num_acked(self):
        with self.lock:
            return len(self.acked_values)

    @property
    def num_not_acked(self):
        with self.lock:
            return len(self.not_acked_values)

    def stop_node(self, node):
        node.account.kill_process("VerifiableProducer", allow_fail=False)
        if self.worker_threads is None:
            return

        # block until the corresponding thread exits
        if len(self.worker_threads) >= self.idx(node):
            # Need to guard this because stop is preemptively called before the worker threads are added and started
            self.worker_threads[self.idx(node) - 1].join()

    def clean_node(self, node):
        node.account.kill_process("VerifiableProducer", clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf /mnt/producer.log /mnt/verifiable_producer.properties", allow_fail=False)
        self.security_config.clean_node(node)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None
