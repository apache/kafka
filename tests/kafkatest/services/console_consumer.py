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

import itertools
import os

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.version import DEV_BRANCH, LATEST_0_8_2, LATEST_0_9, LATEST_0_10_0, V_0_9_0_0, V_0_10_0_0, V_0_11_0_0, V_2_0_0

"""
The console consumer is a tool that reads data from Kafka and outputs it to standard output.
"""


class ConsoleConsumer(KafkaPathResolverMixin, JmxMixin, BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/console_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "console_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "console_consumer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "console_consumer.properties")
    JMX_TOOL_LOG = os.path.join(PERSISTENT_ROOT, "jmx_tool.log")
    JMX_TOOL_ERROR_LOG = os.path.join(PERSISTENT_ROOT, "jmx_tool.err.log")

    logs = {
        "consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "consumer_log": {
            "path": LOG_FILE,
            "collect_default": True},
        "jmx_log": {
            "path" : JMX_TOOL_LOG,
            "collect_default": False},
        "jmx_err_log": {
            "path": JMX_TOOL_ERROR_LOG,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, group_id="test-consumer-group", new_consumer=True,
                 message_validator=None, from_beginning=True, consumer_timeout_ms=None, version=DEV_BRANCH,
                 client_id="console-consumer", print_key=False, jmx_object_names=None, jmx_attributes=None,
                 enable_systest_events=False, stop_timeout_sec=15, print_timestamp=False,
                 isolation_level="read_uncommitted"):
        """
        Args:
            context:                    standard context
            num_nodes:                  number of nodes to use (this should be 1)
            kafka:                      kafka service
            topic:                      consume from this topic
            new_consumer:               use new Kafka consumer if True
            message_validator:          function which returns message or None
            from_beginning:             consume from beginning if True, else from the end
            consumer_timeout_ms:        corresponds to consumer.timeout.ms. consumer process ends if time between
                                        successively consumed messages exceeds this timeout. Setting this and
                                        waiting for the consumer to stop is a pretty good way to consume all messages
                                        in a topic.
            print_key                   if True, print each message's key as well
            enable_systest_events       if True, console consumer will print additional lifecycle-related information
                                        only available in 0.10.0 and later.
            stop_timeout_sec            After stopping a node, wait up to stop_timeout_sec for the node to stop,
                                        and the corresponding background thread to finish successfully.
            print_timestamp             if True, print each message's timestamp as well
            isolation_level             How to handle transactional messages.
        """
        JmxMixin.__init__(self, num_nodes=num_nodes, jmx_object_names=jmx_object_names, jmx_attributes=(jmx_attributes or []),
                          root=ConsoleConsumer.PERSISTENT_ROOT)
        BackgroundThreadService.__init__(self, context, num_nodes)
        self.kafka = kafka
        self.new_consumer = new_consumer
        self.group_id = group_id
        self.args = {
            'topic': topic,
        }

        self.consumer_timeout_ms = consumer_timeout_ms
        for node in self.nodes:
            node.version = version

        self.from_beginning = from_beginning
        self.message_validator = message_validator
        self.messages_consumed = {idx: [] for idx in range(1, num_nodes + 1)}
        self.clean_shutdown_nodes = set()
        self.client_id = client_id
        self.print_key = print_key
        self.log_level = "TRACE"
        self.stop_timeout_sec = stop_timeout_sec

        self.isolation_level = isolation_level
        self.enable_systest_events = enable_systest_events
        if self.enable_systest_events:
            # Only available in 0.10.0 and up
            assert version >= V_0_10_0_0

        self.print_timestamp = print_timestamp

    def prop_file(self, node):
        """Return a string which can be used to create a configuration file appropriate for the given node."""
        # Process client configuration
        prop_file = self.render('console_consumer.properties')
        if hasattr(node, "version") and node.version <= LATEST_0_8_2:
            # in 0.8.2.X and earlier, console consumer does not have --timeout-ms option
            # instead, we have to pass it through the config file
            prop_file += "\nconsumer.timeout.ms=%s\n" % str(self.consumer_timeout_ms)

        # Add security properties to the config. If security protocol is not specified,
        # use the default in the template properties.
        self.security_config = self.kafka.security_config.client_config(prop_file, node)
        self.security_config.setup_node(node)

        prop_file += str(self.security_config)
        return prop_file

    def start_cmd(self, node):
        """Return the start command appropriate for the given node."""
        args = self.args.copy()
        args['zk_connect'] = self.kafka.zk_connect_setting()
        args['stdout'] = ConsoleConsumer.STDOUT_CAPTURE
        args['stderr'] = ConsoleConsumer.STDERR_CAPTURE
        args['log_dir'] = ConsoleConsumer.LOG_DIR
        args['log4j_config'] = ConsoleConsumer.LOG4J_CONFIG
        args['config_file'] = ConsoleConsumer.CONFIG_FILE
        args['stdout'] = ConsoleConsumer.STDOUT_CAPTURE
        args['jmx_port'] = self.jmx_port
        args['console_consumer'] = self.path.script("kafka-console-consumer.sh", node)
        args['broker_list'] = self.kafka.bootstrap_servers(self.security_config.security_protocol)
        args['kafka_opts'] = self.security_config.kafka_opts

        cmd = "export JMX_PORT=%(jmx_port)s; " \
              "export LOG_DIR=%(log_dir)s; " \
              "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j_config)s\"; " \
              "export KAFKA_OPTS=%(kafka_opts)s; " \
              "%(console_consumer)s " \
              "--topic %(topic)s --consumer.config %(config_file)s" % args

        if self.new_consumer:
            assert node.version >= V_0_9_0_0, \
                "new_consumer is only supported if version >= 0.9.0.0, version %s" % str(node.version)
            if node.version <= LATEST_0_10_0:
                cmd += " --new-consumer"
            cmd += " --bootstrap-server %(broker_list)s" % args
            if node.version >= V_0_11_0_0:
                cmd += " --isolation-level %s" % self.isolation_level
        else:
            assert node.version < V_2_0_0, \
                "new_consumer==false is only supported if version < 2.0.0, version %s" % str(node.version)
            cmd += " --zookeeper %(zk_connect)s" % args

        if self.from_beginning:
            cmd += " --from-beginning"

        if self.consumer_timeout_ms is not None:
            # version 0.8.X and below do not support --timeout-ms option
            # This will be added in the properties file instead
            if node.version > LATEST_0_8_2:
                cmd += " --timeout-ms %s" % self.consumer_timeout_ms

        if self.print_key:
            cmd += " --property print.key=true"

        if self.print_timestamp:
            cmd += " --property print.timestamp=true"

        # LoggingMessageFormatter was introduced after 0.9
        if node.version > LATEST_0_9:
            cmd += " --formatter kafka.tools.LoggingMessageFormatter"

        if self.enable_systest_events:
            # enable systest events is only available in 0.10.0 and later
            # check the assertion here as well, in case node.version has been modified
            assert node.version >= V_0_10_0_0
            cmd += " --enable-systest-events"

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s &" % args
        return cmd

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i console_consumer | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % ConsoleConsumer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload config file
        self.logger.info("console_consumer.properties:")

        prop_file = self.prop_file(node)
        self.logger.info(prop_file)
        node.account.create_file(ConsoleConsumer.CONFIG_FILE, prop_file)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=ConsoleConsumer.LOG_FILE)
        node.account.create_file(ConsoleConsumer.LOG4J_CONFIG, log_config)

        # Run and capture output
        cmd = self.start_cmd(node)
        self.logger.debug("Console consumer %d command: %s", idx, cmd)

        consumer_output = node.account.ssh_capture(cmd, allow_fail=False)

        with self.lock:
            self._init_jmx_attributes()
            self.logger.debug("collecting following jmx objects: %s", self.jmx_object_names)
            self.start_jmx_tool(idx, node)

        for line in consumer_output:
            msg = line.strip()
            if msg == "shutdown_complete":
                # Note that we can only rely on shutdown_complete message if running 0.10.0 or greater
                if node in self.clean_shutdown_nodes:
                    raise Exception("Unexpected shutdown event from consumer, already shutdown. Consumer index: %d" % idx)
                self.clean_shutdown_nodes.add(node)
            else:
                if self.message_validator is not None:
                    msg = self.message_validator(msg)
                if msg is not None:
                    self.messages_consumed[idx].append(msg)

        with self.lock:
            self.read_jmx_output(idx, node)

    def start_node(self, node):
        BackgroundThreadService.start_node(self, node)

    def stop_node(self, node):
        node.account.kill_process("console_consumer", allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        JmxMixin.clean_node(self, node)
        node.account.kill_process("java", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s" % ConsoleConsumer.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def has_partitions_assigned(self, node):
        if self.new_consumer is False:
            return False
        idx = self.idx(node)
        with self.lock:
            self._init_jmx_attributes()
            self.start_jmx_tool(idx, node)
            self.read_jmx_output(idx, node)
        if not self.assigned_partitions_jmx_attr in self.maximum_jmx_value:
            return False
        self.logger.debug("Number of partitions assigned %f" % self.maximum_jmx_value[self.assigned_partitions_jmx_attr])
        return self.maximum_jmx_value[self.assigned_partitions_jmx_attr] > 0.0

    def _init_jmx_attributes(self):
        # Must hold lock
        if self.new_consumer:
            # We use a flag to track whether we're using this automatically generated ID because the service could be
            # restarted multiple times and the client ID may be changed.
            if getattr(self, '_automatic_metrics', False) or not self.jmx_object_names:
                self._automatic_metrics = True
                self.jmx_object_names = ["kafka.consumer:type=consumer-coordinator-metrics,client-id=%s" % self.client_id]
                self.jmx_attributes = ["assigned-partitions"]
                self.assigned_partitions_jmx_attr = "kafka.consumer:type=consumer-coordinator-metrics,client-id=%s:assigned-partitions" % self.client_id

