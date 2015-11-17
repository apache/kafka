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

from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.performance import PerformanceService
import itertools
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.kafka.directory import kafka_dir
from kafkatest.services.kafka.version import TRUNK, LATEST_0_8_2

class ProducerPerformanceService(JmxMixin, PerformanceService):

    logs = {
        "producer_performance_log": {
            "path": "/mnt/producer-performance.log",
            "collect_default": True},
    }

    def __init__(self, context, num_nodes, kafka, topic, num_records, record_size, throughput, version=TRUNK, settings={},
                 intermediate_stats=False, client_id="producer-performance", jmx_object_names=None, jmx_attributes=[]):
        JmxMixin.__init__(self, num_nodes, jmx_object_names, jmx_attributes)
        PerformanceService.__init__(self, context, num_nodes)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()
        self.args = {
            'topic': topic,
            'kafka_opts': self.security_config.kafka_opts,
            'num_records': num_records,
            'record_size': record_size,
            'throughput': throughput
        }
        self.settings = settings
        self.intermediate_stats = intermediate_stats
        self.client_id = client_id

        for node in self.nodes:
            node.version = version

    def start_cmd(self, node):
        args = self.args.copy()
        args.update({
            'bootstrap_servers': self.kafka.bootstrap_servers(self.security_config.security_protocol),
            'jmx_port': self.jmx_port,
            'client_id': self.client_id,
            'kafka_directory': kafka_dir(node)
            })

        cmd = ""
        if node.version > LATEST_0_8_2:
            cmd += "JMX_PORT=%(jmx_port)d KAFKA_OPTS=%(kafka_opts)s /opt/%(kafka_directory)s/bin/kafka-producer-perf-test.sh " \
                  "--topic %(topic)s --num-records %(num_records)d --record-size %(record_size)d --throughput %(throughput)d --producer-props bootstrap.servers=%(bootstrap_servers)s client.id=%(client_id)s" % args

            self.security_config.setup_node(node)
            if self.security_config.security_protocol != SecurityConfig.PLAINTEXT:
                self.settings.update(self.security_config.properties)
            for key, value in self.settings.items():
                cmd += " %s=%s" % (str(key), str(value))
        else:
            # 0.8.X has different option names
            cmd += "JMX_PORT=%(jmx_port)d /opt/%(kafka_directory)s/bin/kafka-producer-perf-test.sh " \
                  "--topic %(topic)s --messages %(num_records)d --message-size %(record_size)d --new-producer --broker-list %(bootstrap_servers)s" % args

        cmd += " | tee /mnt/producer-performance.log"
        return cmd

    def _worker(self, idx, node):

        cmd = self.start_cmd(node)
        self.logger.debug("Producer performance %d command: %s", idx, cmd)

        last = None
        producer_output = node.account.ssh_capture(cmd)
        first_line = next(producer_output, None)

        if first_line is not None:
            self.start_jmx_tool(idx, node)
            for line in itertools.chain([first_line], producer_output):
                if self.intermediate_stats:
                    try:
                        self.stats[idx-1].append(self.parse_stats(line, node.version))
                    except:
                        # Sometimes there are extraneous log messages
                        pass

                last = line
            try:
                self.results[idx-1] = self.parse_stats(last, node.version)
            except:
                raise Exception("Unable to parse aggregate performance statistics on node %d: %s" % (idx, last))
            self.read_jmx_output(idx, node)

    def parse_stats(self, line, version=TRUNK):

        if version > LATEST_0_8_2:
            parts = line.split(',')
            return {
                'records': int(parts[0].split()[0]),
                'records_per_sec': float(parts[1].split()[0]),
                'mbps': float(parts[1].split('(')[1].split()[0]),
                'latency_avg_ms': float(parts[2].split()[0]),
                'latency_max_ms': float(parts[3].split()[0]),
                'latency_50th_ms': float(parts[4].split()[0]),
                'latency_95th_ms': float(parts[5].split()[0]),
                'latency_99th_ms': float(parts[6].split()[0]),
                'latency_999th_ms': float(parts[7].split()[0]),
            }
        else:
            parts = line.split(',')
            return {
                'records_per_sec': float(parts[8].split()[0]),
                'mbps': float(parts[6].split()[0])
            }
