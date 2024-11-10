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
from kafkatest.version import V_2_5_0, DEV_BRANCH


class ConsumerPerformanceService(PerformanceService):
    """
        See ConsumerPerformance tool as the source of truth on these settings, but for reference:

        "zookeeper" "The connection string for the zookeeper connection in the form host:port. Multiple URLS can
                     be given to allow fail-over. This option is only used with the old consumer."

        "topic", "REQUIRED: The topic to consume from."

        "group", "The group id to consume on."

        "fetch-size", "The amount of data to fetch in a single request."

        "from-latest", "If the consumer does not already have an establishedoffset to consume from,
                        start with the latest message present in the log rather than the earliest message."

        "socket-buffer-size", "The size of the tcp RECV size."

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

    def __init__(self, context, num_nodes, kafka, topic, messages, version=DEV_BRANCH, settings={}):
        super(ConsumerPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()
        self.topic = topic
        self.messages = messages
        self.settings = settings

        # These less-frequently used settings can be updated manually after instantiation
        self.fetch_size = None
        self.socket_buffer_size = None
        self.group = None
        self.from_latest = None

        for node in self.nodes:
            node.version = version

    def args(self, version):
        """Dictionary of arguments used to start the Consumer Performance script."""
        args = {
            'topic': self.topic,
            'messages': self.messages
        }

        if version < V_2_5_0:
            args['broker-list'] = self.kafka.bootstrap_servers(self.security_config.security_protocol)
        else:
            args['bootstrap-server'] = self.kafka.bootstrap_servers(self.security_config.security_protocol)

        if self.fetch_size is not None:
            args['fetch-size'] = self.fetch_size

        if self.socket_buffer_size is not None:
            args['socket-buffer-size'] = self.socket_buffer_size

        if self.group is not None:
            args['group'] = self.group

        if self.from_latest:
            args['from-latest'] = ""

        return args

    def start_cmd(self, node):
        cmd = fix_opts_for_new_jvm(node)
        cmd += "export LOG_DIR=%s;" % ConsumerPerformanceService.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\";" % ConsumerPerformanceService.LOG4J_CONFIG
        cmd += " %s" % self.path.script("kafka-consumer-perf-test.sh", node)
        for key, value in self.args(node.version).items():
            cmd += " --%s %s" % (key, value)

        cmd += " --consumer.config %s" % ConsumerPerformanceService.CONFIG_FILE

        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {'stdout': ConsumerPerformanceService.STDOUT_CAPTURE,
                                                        'stderr': ConsumerPerformanceService.STDERR_CAPTURE}
        return cmd

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
        if last is not None:
            parts = last.split(',')
            self.results[idx-1] = {
                'total_mb': float(parts[2]),
                'mbps': float(parts[3]),
                'records_per_sec': float(parts[5]),
            }
