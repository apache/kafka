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

from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH, V_0_9_0_0, LATEST_0_10_0


class ConsumerPerformanceService(PerformanceService):
    """
        See ConsumerPerformance.scala as the source of truth on these settings, but for reference:

        "zookeeper" "The connection string for the zookeeper connection in the form host:port. Multiple URLS can
                     be given to allow fail-over. This option is only used with the old consumer."

        "broker-list", "A broker list to use for connecting if using the new consumer."

        "topic", "REQUIRED: The topic to consume from."

        "group", "The group id to consume on."

        "fetch-size", "The amount of data to fetch in a single request."

        "from-latest", "If the consumer does not already have an establishedoffset to consume from,
                        start with the latest message present in the log rather than the earliest message."

        "socket-buffer-size", "The size of the tcp RECV size."

        "threads", "Number of processing threads."

        "num-fetch-threads", "Number of fetcher threads. Defaults to 1"

        "new-consumer", "Use the new consumer implementation."
        "consumer.config", "Consumer config properties file."
    """

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/consumer_performance"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "consumer_performance.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "consumer_performance.stderr")
    LOG_FILE = os.path.join(LOG_DIR, "consumer_performance.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "consumer.properties")

    logs = {
        "consumer_performance_output": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "consumer_performance_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "consumer_performance_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, messages, version=DEV_BRANCH, new_consumer=True, settings={}):
        super(ConsumerPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()
        self.topic = topic
        self.messages = messages
        self.new_consumer = new_consumer
        self.settings = settings

        assert version >= V_0_9_0_0 or (not new_consumer), \
            "new_consumer is only supported if version >= 0.9.0.0, version %s" % str(version)

        security_protocol = self.security_config.security_protocol
        assert version >= V_0_9_0_0 or security_protocol == SecurityConfig.PLAINTEXT, \
            "Security protocol %s is only supported if version >= 0.9.0.0, version %s" % (self.security_config, str(version))

        # These less-frequently used settings can be updated manually after instantiation
        self.fetch_size = None
        self.socket_buffer_size = None
        self.threads = None
        self.num_fetch_threads = None
        self.group = None
        self.from_latest = None

        for node in self.nodes:
            node.version = version

    def args(self, version):
        """Dictionary of arguments used to start the Consumer Performance script."""
        args = {
            'topic': self.topic,
            'messages': self.messages,
        }

        if self.new_consumer:
            if version <= LATEST_0_10_0:
                args['new-consumer'] = ""
            args['broker-list'] = self.kafka.bootstrap_servers(self.security_config.security_protocol)
        else:
            args['zookeeper'] = self.kafka.zk_connect_setting()

        if self.fetch_size is not None:
            args['fetch-size'] = self.fetch_size

        if self.socket_buffer_size is not None:
            args['socket-buffer-size'] = self.socket_buffer_size

        if self.threads is not None:
            args['threads'] = self.threads

        if self.num_fetch_threads is not None:
            args['num-fetch-threads'] = self.num_fetch_threads

        if self.group is not None:
            args['group'] = self.group

        if self.from_latest:
            args['from-latest'] = ""

        return args

    def start_cmd(self, node):
        cmd = "export LOG_DIR=%s;" % ConsumerPerformanceService.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\";" % ConsumerPerformanceService.LOG4J_CONFIG
        cmd += " %s" % self.path.script("kafka-consumer-perf-test.sh", node)
        for key, value in self.args(node.version).items():
            cmd += " --%s %s" % (key, value)

        if node.version >= V_0_9_0_0:
            # This is only used for security settings
            cmd += " --consumer.config %s" % ConsumerPerformanceService.CONFIG_FILE

        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {'stdout': ConsumerPerformanceService.STDOUT_CAPTURE,
                                                        'stderr': ConsumerPerformanceService.STDERR_CAPTURE}
        return cmd

    def parse_results(self, line, version):
        parts = line.split(',')
        if version >= V_0_9_0_0:
            result = {
                'total_mb': float(parts[2]),
                'mbps': float(parts[3]),
                'records_per_sec': float(parts[5]),
            }
        else:
            result = {
                'total_mb': float(parts[3]),
                'mbps': float(parts[4]),
                'records_per_sec': float(parts[6]),
            }
        return result

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % ConsumerPerformanceService.PERSISTENT_ROOT, allow_fail=False)

        log_config = self.render('tools_log4j.properties', log_file=ConsumerPerformanceService.LOG_FILE)
        node.account.create_file(ConsumerPerformanceService.LOG4J_CONFIG, log_config)
        node.account.create_file(ConsumerPerformanceService.CONFIG_FILE, str(self.security_config))
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("Consumer performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            last = line

        # Parse and save the last line's information
        self.results[idx-1] = self.parse_results(last, node.version)
