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

from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig

from kafkatest.services.kafka.directory import kafka_dir
from kafkatest.services.kafka.version import TRUNK, V_0_9_0_0, V_0_10_0_0


class EndToEndLatencyService(PerformanceService):
    MESSAGE_BYTES = 21  # 0.8.X messages are fixed at 21 bytes, so we'll match that for other versions

    logs = {
        "end_to_end_latency_log": {
            "path": "/mnt/end-to-end-latency.log",
            "collect_default": True},
    }

    def __init__(self, context, num_nodes, kafka, topic, num_records, compression_type="none", version=TRUNK, acks=1):
        super(EndToEndLatencyService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()

        security_protocol = self.security_config.security_protocol
        assert version >= V_0_9_0_0 or security_protocol == SecurityConfig.PLAINTEXT, \
            "Security protocol %s is only supported if version >= 0.9.0.0, version %s" % (self.security_config, str(version))
        assert version >= TRUNK or compression_type == "none", \
            "Compression type %s is only supported in trunk, version %s" % (compression_type, str(version))

        self.args = {
            'topic': topic,
            'num_records': num_records,
            'acks': acks,
            'compression_type': compression_type,
            'kafka_opts': self.security_config.kafka_opts,
            'message_bytes': EndToEndLatencyService.MESSAGE_BYTES
        }

        for node in self.nodes:
            node.version = version

    @property
    def security_config_file(self):
        if self.security_config.security_protocol != SecurityConfig.PLAINTEXT:
            security_config_file = SecurityConfig.CONFIG_DIR + "/security.properties"
        else:
            security_config_file = ""
        return security_config_file

    def start_cmd(self, node):
        args = self.args.copy()
        args.update({
            'zk_connect': self.kafka.zk.connect_setting(),
            'bootstrap_servers': self.kafka.bootstrap_servers(self.security_config.security_protocol),
            'security_config_file': self.security_config_file,
            'kafka_dir': kafka_dir(node)
        })

        if node.version >= V_0_9_0_0:
            cmd = "KAFKA_OPTS=%(kafka_opts)s /opt/%(kafka_dir)s/bin/kafka-run-class.sh kafka.tools.EndToEndLatency " % args
            cmd_args = "%(bootstrap_servers)s %(topic)s %(num_records)d %(acks)d %(message_bytes)d %(security_config_file)s"
            if node.version >= V_0_10_0_0:
                cmd_args += " %(compression_type)s"
            cmd += cmd_args % args
        else:
            # Set fetch max wait to 0 to match behavior in later versions
            cmd = "KAFKA_OPTS=%(kafka_opts)s /opt/%(kafka_dir)s/bin/kafka-run-class.sh kafka.tools.TestEndToEndLatency " % args
            cmd += "%(bootstrap_servers)s %(zk_connect)s %(topic)s %(num_records)d 0 %(acks)d" % args

        cmd += " | tee /mnt/end-to-end-latency.log"

        return cmd

    def _worker(self, idx, node):
        self.security_config.setup_node(node)
        if self.security_config.security_protocol != SecurityConfig.PLAINTEXT:
            node.account.create_file(self.security_config_file, str(self.security_config))

        cmd = self.start_cmd(node)
        self.logger.debug("End-to-end latency %d command: %s", idx, cmd)
        results = {}
        for line in node.account.ssh_capture(cmd):
            if line.startswith("Avg latency:"):
                results['latency_avg_ms'] = float(line.split()[2])
            if line.startswith("Percentiles"):
                results['latency_50th_ms'] = float(line.split()[3][:-1])
                results['latency_99th_ms'] = float(line.split()[6][:-1])
                results['latency_999th_ms'] = float(line.split()[9])
        self.results[idx-1] = results
