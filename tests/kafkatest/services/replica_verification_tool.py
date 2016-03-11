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

from kafkatest.services.kafka.directory import kafka_dir
from kafkatest.services.security.security_config import SecurityConfig

import re

class ReplicaVerificationTool(BackgroundThreadService):

    logs = {
        "producer_log": {
            "path": "/mnt/replica_verification_tool.log",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, report_interval_ms, security_protocol="PLAINTEXT"):
        super(ReplicaVerificationTool, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.report_interval_ms = report_interval_ms
        self.security_protocol = security_protocol
        self.security_config = SecurityConfig(security_protocol)
        self.lagged_values = []
        self.zero_lagged_values = []

    def _worker(self, idx, node):
        cmd = self.start_cmd(node)
        self.logger.debug("ReplicaVerificationTool %d command: %s" % (idx, cmd))
        self.security_config.setup_node(node)
        for line in node.account.ssh_capture(cmd):
            lag = re.search('.*max lag is (.+?) for partition', line)
            if lag:
                if int(lag.group(1)) > 0:
                    self.logger.info("Appending to lags: %s" % line)
                    self.lagged_values.append(line)
                else:
                    self.logger.info("Appending to zero lags: %s" % line)
                    self.zero_lagged_values.append(line)

    def num_lags(self):
        return len(self.lagged_values)

    def num_zero_lags(self):
        return len(self.zero_lagged_values)

    def start_cmd(self, node):
        cmd = "/opt/%s/bin/" % kafka_dir(node)
        cmd += "kafka-run-class.sh kafka.tools.ReplicaVerificationTool"
        cmd += " --broker-list %s --topic-white-list %s --time -2 --report-interval-ms %s" % (self.kafka.bootstrap_servers(self.security_protocol), self.topic, self.report_interval_ms)

        cmd += " 2>> /mnt/replica_verification_tool.log | tee -a /mnt/replica_verification_tool.log &"
        return cmd

    def stop_node(self, node):
        node.account.kill_process("ReplicaVerificationTool", allow_fail=False)
        if self.worker_threads is None:
            return

        # block until the corresponding thread exits
        if len(self.worker_threads) >= self.idx(node):
            # Need to guard this because stop is preemptively called before the worker threads are added and started
            self.worker_threads[self.idx(node) - 1].join()

    def clean_node(self, node):
        node.account.kill_process("ReplicaVerificationTool", clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf /mnt/replica_verification_tool.log", allow_fail=False)
