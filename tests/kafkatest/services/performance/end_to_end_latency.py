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

from kafkatest.services.kafka.util import fix_opts_for_new_jvm
from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import get_version, V_3_4_0, DEV_BRANCH



class EndToEndLatencyService(PerformanceService):
    MESSAGE_BYTES = 21  # 0.8.X messages are fixed at 21 bytes, so we'll match that for other versions

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/end_to_end_latency"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "end_to_end_latency.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "end_to_end_latency.stderr")
    LOG_FILE = os.path.join(LOG_DIR, "end_to_end_latency.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "client.properties")

    logs = {
        "end_to_end_latency_output": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "end_to_end_latency_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "end_to_end_latency_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, num_records, compression_type="none", version=DEV_BRANCH, acks=1):
        super(EndToEndLatencyService, self).__init__(context, num_nodes,
                                                     root=EndToEndLatencyService.PERSISTENT_ROOT)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()
        self.version = ''

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

    def start_cmd(self, node):
        args = self.args.copy()
        self.version = get_version(node)
        args.update({
            'bootstrap_servers': self.kafka.bootstrap_servers(self.security_config.security_protocol),
            'config_file': EndToEndLatencyService.CONFIG_FILE,
            'kafka_run_class': self.path.script("kafka-run-class.sh", node),
            'java_class_name': self.java_class_name()
        })

        cmd = fix_opts_for_new_jvm(node)
        cmd += "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % EndToEndLatencyService.LOG4J_CONFIG
        cmd += "KAFKA_OPTS=%(kafka_opts)s %(kafka_run_class)s %(java_class_name)s " % args
        cmd += "%(bootstrap_servers)s %(topic)s %(num_records)d %(acks)d %(message_bytes)d %(config_file)s" % args

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {'stdout': EndToEndLatencyService.STDOUT_CAPTURE,
                                                        'stderr': EndToEndLatencyService.STDERR_CAPTURE}

        return cmd

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % EndToEndLatencyService.PERSISTENT_ROOT, allow_fail=False)

        log_config = self.render('tools_log4j.properties', log_file=EndToEndLatencyService.LOG_FILE)

        node.account.create_file(EndToEndLatencyService.LOG4J_CONFIG, log_config)
        client_config = str(self.security_config)
        client_config += "compression_type=%(compression_type)s" % self.args
        node.account.create_file(EndToEndLatencyService.CONFIG_FILE, client_config)

        self.security_config.setup_node(node)

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

    def java_class_name(self):
        if self.version <= V_3_4_0:
            return "kafka.tools.EndToEndLatency"
        else:
            return "org.apache.kafka.tools.EndToEndLatency"
