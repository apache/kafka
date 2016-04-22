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
        self.partition_lag = {}

    def _worker(self, idx, node):
        cmd = self.start_cmd(node)
        self.logger.debug("ReplicaVerificationTool %d command: %s" % (idx, cmd))
        self.security_config.setup_node(node)
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("Parsing line:{}".format(line))

            parsed = re.search('.*max lag is (.+?) for partition \[(.+?)\] at', line)
            if parsed:
                lag = int(parsed.group(1))
                topic_partition = parsed.group(2)
                self.logger.debug("Setting max lag for {} as {}".format(topic_partition, lag))
                self.partition_lag[topic_partition] = lag

    def get_lag_for_partition(self, topic, partition):
        """
        Get latest lag for given topic-partition

        Args:
            topic:          a topic
            partition:      a partition of the topic
        """
        topic_partition = topic + ',' + str(partition)
        lag = self.partition_lag[topic_partition]
        self.logger.debug("Retuning lag for {} as {}".format(topic_partition, lag))
        return lag

    def start_cmd(self, node):
        cmd = "/opt/%s/bin/" % kafka_dir(node)
        cmd += "kafka-run-class.sh kafka.tools.ReplicaVerificationTool"
        cmd += " --broker-list %s --topic-white-list %s --time -2 --report-interval-ms %s" % (self.kafka.bootstrap_servers(self.security_protocol), self.topic, self.report_interval_ms)

        cmd += " 2>> /mnt/replica_verification_tool.log | tee -a /mnt/replica_verification_tool.log &"
        return cmd

    def stop_node(self, node):
        node.account.kill_process("java", clean_shutdown=True, allow_fail=True)

    def clean_node(self, node):
        node.account.kill_process("java", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/replica_verification_tool.log", allow_fail=False)