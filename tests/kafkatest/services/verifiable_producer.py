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
from kafkatest.utils.security_config import SecurityConfig

import json


class VerifiableProducer(BackgroundThreadService):

    CONFIG_FILE = "/mnt/verifiable_producer.properties"
    logs = {
        "producer_log": {
            "path": "/mnt/producer.log",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, security_protocol=None, max_messages=-1, throughput=100000):
        super(VerifiableProducer, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.max_messages = max_messages
        self.throughput = throughput

        self.acked_values = []
        self.not_acked_values = []

        self.prop_file = ""
        self.security_config = SecurityConfig(security_protocol, self.prop_file)
        self.security_protocol = self.security_config.security_protocol
        self.prop_file += str(self.security_config)

    def _worker(self, idx, node):
        # Create and upload config file
        self.logger.info("verifiable_producer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableProducer.CONFIG_FILE, self.prop_file)
        self.security_config.setup_node(node)

        cmd = self.start_cmd
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

    @property
    def start_cmd(self):
        cmd = "/opt/kafka/bin/kafka-verifiable-producer.sh" \
              " --topic %s --broker-list %s" % (self.topic, self.kafka.bootstrap_servers())
        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        if self.throughput > 0:
            cmd += " --throughput %s" % str(self.throughput)

        cmd += " --producer.config %s" % VerifiableProducer.CONFIG_FILE
        cmd += " 2>> /mnt/producer.log | tee -a /mnt/producer.log &"
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
