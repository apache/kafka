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
from kafkatest.utils.security_config import SecurityConfig


class EndToEndLatencyService(PerformanceService):

    logs = {
        "end_to_end_latency_log": {
            "path": "/mnt/end-to-end-latency.log",
            "collect_default": True},
    }

    def __init__(self, context, num_nodes, kafka, security_protocol, topic, num_records, consumer_fetch_max_wait=100, acks=1):
        super(EndToEndLatencyService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.security_config = SecurityConfig(security_protocol)
        self.security_protocol = security_protocol
        self.args = {
            'topic': topic,
            'num_records': num_records,
            'consumer_fetch_max_wait': consumer_fetch_max_wait,
            'acks': acks
        }

    def _worker(self, idx, node):
        args = self.args.copy()
        self.security_config.setup_node(node)
        if self.security_protocol == SecurityConfig.SSL:
            ssl_config_file = SecurityConfig.SSL_DIR + "/security.properties"
            node.account.create_file(ssl_config_file, str(self.security_config))
        else:
            ssl_config_file = ""
        args.update({
            'zk_connect': self.kafka.zk.connect_setting(),
            'bootstrap_servers': self.kafka.bootstrap_servers(),
            'ssl_config_file': ssl_config_file
        })

        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.EndToEndLatency "\
              "%(bootstrap_servers)s %(topic)s %(num_records)d "\
              "%(acks)d 20 %(ssl_config_file)s" % args

        cmd += " | tee /mnt/end-to-end-latency.log"

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
