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
import time
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import  TOOLS_JAR_NAME, TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH, V_0_9_0_0


class ProducerPerformanceService(JmxMixin, PerformanceService):

    PERSISTENT_ROOT = "/mnt/producer_performance"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "producer_performance.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "producer_performance.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "producer_performance.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")

    def __init__(self, context, num_nodes, kafka, topic, num_records, record_size, throughput, version=DEV_BRANCH, settings=None,
                 intermediate_stats=False, client_id="producer-performance", jmx_object_names=None, jmx_attributes=None):

        JmxMixin.__init__(self, num_nodes, jmx_object_names, jmx_attributes or [])
        PerformanceService.__init__(self, context, num_nodes)

        self.logs = {
            "producer_performance_stdout": {
                "path": ProducerPerformanceService.STDOUT_CAPTURE,
                "collect_default": True},
            "producer_performance_stderr": {
                "path": ProducerPerformanceService.STDERR_CAPTURE,
                "collect_default": True},
            "producer_performance_log": {
                "path": ProducerPerformanceService.LOG_FILE,
                "collect_default": True},
            "jmx_log": {
                "path": "/mnt/jmx_tool.log",
                "collect_default": jmx_object_names is not None
            }

        }

        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()

        security_protocol = self.security_config.security_protocol
        assert version >= V_0_9_0_0 or security_protocol == SecurityConfig.PLAINTEXT, \
            "Security protocol %s is only supported if version >= 0.9.0.0, version %s" % (self.security_config, str(version))

        self.args = {
            'topic': topic,
            'kafka_opts': self.security_config.kafka_opts,
            'num_records': num_records,
            'record_size': record_size,
            'throughput': throughput
        }
        self.settings = settings or {}
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
            'kafka_run_class': self.path.script("kafka-run-class.sh", node)
            })

        cmd = ""

        if node.version < DEV_BRANCH:
            # In order to ensure more consistent configuration between versions, always use the ProducerPerformance
            # tool from the development branch
            tools_jar = self.path.jar(TOOLS_JAR_NAME, DEV_BRANCH)
            tools_dependant_libs_jar = self.path.jar(TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)

            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % tools_jar
            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % tools_dependant_libs_jar
            cmd += "export CLASSPATH; "

        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % ProducerPerformanceService.LOG4J_CONFIG
        cmd += "JMX_PORT=%(jmx_port)d KAFKA_OPTS=%(kafka_opts)s KAFKA_HEAP_OPTS=\"-XX:+HeapDumpOnOutOfMemoryError\" %(kafka_run_class)s org.apache.kafka.tools.ProducerPerformance " \
              "--topic %(topic)s --num-records %(num_records)d --record-size %(record_size)d --throughput %(throughput)d --producer-props bootstrap.servers=%(bootstrap_servers)s client.id=%(client_id)s" % args

        self.security_config.setup_node(node)
        if self.security_config.security_protocol != SecurityConfig.PLAINTEXT:
            self.settings.update(self.security_config.properties)

        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        cmd += " 2>>%s | tee %s" % (ProducerPerformanceService.STDERR_CAPTURE, ProducerPerformanceService.STDOUT_CAPTURE)
        return cmd

    def pids(self, node):
        try:
            cmd = "jps | grep -i ProducerPerformance | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):

        node.account.ssh("mkdir -p %s" % ProducerPerformanceService.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=ProducerPerformanceService.LOG_FILE)
        node.account.create_file(ProducerPerformanceService.LOG4J_CONFIG, log_config)

        cmd = self.start_cmd(node)
        self.logger.debug("Producer performance %d command: %s", idx, cmd)

        # start ProducerPerformance process
        start = time.time()
        producer_output = node.account.ssh_capture(cmd)
        wait_until(lambda: self.alive(node), timeout_sec=20, err_msg="ProducerPerformance failed to start")
        # block until there is at least one line of output
        first_line = next(producer_output, None)
        if first_line is None:
            raise Exception("No output from ProducerPerformance")

        self.start_jmx_tool(idx, node)
        wait_until(lambda: not self.alive(node), timeout_sec=1200, backoff_sec=2, err_msg="ProducerPerformance failed to finish")
        elapsed = time.time() - start
        self.logger.debug("ProducerPerformance process ran for %s seconds" % elapsed)

        self.read_jmx_output(idx, node)

        # parse producer output from file
        last = None
        producer_output = node.account.ssh_capture("cat %s" % ProducerPerformanceService.STDOUT_CAPTURE)
        for line in producer_output:
            if self.intermediate_stats:
                try:
                    self.stats[idx-1].append(self.parse_stats(line))
                except:
                    # Sometimes there are extraneous log messages
                    pass

            last = line
        try:
            self.results[idx-1] = self.parse_stats(last)
        except:
            raise Exception("Unable to parse aggregate performance statistics on node %d: %s" % (idx, last))

    def parse_stats(self, line):

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
