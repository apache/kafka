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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.version import DEV_BRANCH, LATEST_4_1, get_version
from kafkatest.services.kafka.util import fix_opts_for_new_jvm, get_log4j_config_param, get_log4j_config_for_tools

"""
The console share consumer is a tool that reads data from Kafka via a share consumer and outputs 
it to standard output.
"""
class ConsoleShareConsumer(KafkaPathResolverMixin, JmxMixin, BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/console_share_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "console_share_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console_share_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "console_share_consumer.log")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "console_share_consumer.properties")
    JMX_TOOL_LOG = os.path.join(PERSISTENT_ROOT, "jmx_tool.log")
    JMX_TOOL_ERROR_LOG = os.path.join(PERSISTENT_ROOT, "jmx_tool.err.log")

    logs = {
        "share_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "share_consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "share_consumer_log": {
            "path": LOG_FILE,
            "collect_default": True},
        "jmx_log": {
            "path" : JMX_TOOL_LOG,
            "collect_default": False},
        "jmx_err_log": {
            "path": JMX_TOOL_ERROR_LOG,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, group_id="test-share-group",
                 message_validator=None, share_consumer_timeout_ms=None, version=DEV_BRANCH,
                 client_id="console-share-consumer", print_key=False, jmx_object_names=None, jmx_attributes=None,
                 enable_systest_events=False, stop_timeout_sec=35, print_timestamp=False, print_partition=False,
                 jaas_override_variables=None, kafka_opts_override="", client_prop_file_override="", 
                 share_consumer_properties={}, log_level="DEBUG"):
        """
        Args:
            context:                    standard context
            num_nodes:                  number of nodes to use (this should be 1)
            kafka:                      kafka service
            topic:                      consume from this topic
            message_validator:          function which returns message or None
            share_consumer_timeout_ms:  corresponds to share.consumer.timeout.ms. share consumer process ends if time between
                                        successively consumed messages exceeds this timeout. Setting this and
                                        waiting for the share consumer to stop is a pretty good way to consume all messages
                                        in a topic.
            print_timestamp             if True, print each message's timestamp as well
            print_key                   if True, print each message's key as well
            print_partition             if True, print each message's partition as well
            enable_systest_events       if True, console share consumer will print additional lifecycle-related information
                                        only available in 0.10.0 and later.
            stop_timeout_sec            After stopping a node, wait up to stop_timeout_sec for the node to stop,
                                        and the corresponding background thread to finish successfully.
            jaas_override_variables     A dict of variables to be used in the jaas.conf template file
            kafka_opts_override         Override parameters of the KAFKA_OPTS environment variable
            client_prop_file_override   Override client.properties file used by the consumer
            share_consumer_properties   A dict of values to pass in as --command-property key=value. For versions older than KAFKA_4_2_0, these will be passed as --consumer-property key=value
        """
        JmxMixin.__init__(self, num_nodes=num_nodes, jmx_object_names=jmx_object_names, jmx_attributes=(jmx_attributes or []),
                          root=ConsoleShareConsumer.PERSISTENT_ROOT)
        BackgroundThreadService.__init__(self, context, num_nodes)
        self.kafka = kafka
        self.group_id = group_id
        self.args = {
            'topic': topic,
        }

        self.share_consumer_timeout_ms = share_consumer_timeout_ms
        for node in self.nodes:
            node.version = version

        self.message_validator = message_validator
        self.messages_consumed = {idx: [] for idx in range(1, num_nodes + 1)}
        self.clean_shutdown_nodes = set()
        self.client_id = client_id
        self.print_key = print_key
        self.print_partition = print_partition
        self.log_level = log_level
        self.stop_timeout_sec = stop_timeout_sec

        self.enable_systest_events = enable_systest_events

        self.print_timestamp = print_timestamp
        self.jaas_override_variables = jaas_override_variables or {}
        self.kafka_opts_override = kafka_opts_override
        self.client_prop_file_override = client_prop_file_override
        self.share_consumer_properties = share_consumer_properties


    def prop_file(self, node):
        """Return a string which can be used to create a configuration file appropriate for the given node."""
        # Process client configuration
        prop_file = self.render('console_share_consumer.properties')

        # Add security properties to the config. If security protocol is not specified,
        # use the default in the template properties.
        self.security_config = self.kafka.security_config.client_config(prop_file, node, self.jaas_override_variables)
        self.security_config.setup_node(node)

        prop_file += str(self.security_config)
        return prop_file


    def start_cmd(self, node):
        """Return the start command appropriate for the given node."""
        args = self.args.copy()
        args['broker_list'] = self.kafka.bootstrap_servers(self.security_config.security_protocol)
        args['stdout'] = ConsoleShareConsumer.STDOUT_CAPTURE
        args['stderr'] = ConsoleShareConsumer.STDERR_CAPTURE
        args['log_dir'] = ConsoleShareConsumer.LOG_DIR
        args['log4j_param'] = get_log4j_config_param(node)
        args['log4j_config'] = get_log4j_config_for_tools(node)
        args['config_file'] = ConsoleShareConsumer.CONFIG_FILE
        args['stdout'] = ConsoleShareConsumer.STDOUT_CAPTURE
        args['jmx_port'] = self.jmx_port
        args['console_share_consumer'] = self.path.script("kafka-console-share-consumer.sh", node)

        if self.kafka_opts_override:
            args['kafka_opts'] = "\"%s\"" % self.kafka_opts_override
        else:
            args['kafka_opts'] = self.security_config.kafka_opts

        cmd = fix_opts_for_new_jvm(node)
        cmd += "export JMX_PORT=%(jmx_port)s; " \
              "export LOG_DIR=%(log_dir)s; " \
              "export KAFKA_LOG4J_OPTS=\"%(log4j_param)s%(log4j_config)s\"; " \
              "export KAFKA_OPTS=%(kafka_opts)s; " \
              "%(console_share_consumer)s " \
              "--topic %(topic)s " % args

        version = get_version(node)
        command_config_arg = "--command-config" if version.supports_command_config() else "--consumer-config"
        cmd += "%s %s" % (command_config_arg, args['config_file'])
        cmd += " --bootstrap-server %(broker_list)s" % args

        if self.share_consumer_timeout_ms is not None:
            # This will be added in the properties file instead
            cmd += " --timeout-ms %s" % self.share_consumer_timeout_ms

        formatter_property_arg = "--formatter-property" if version.supports_formatter_property() else "--property"
        if self.print_timestamp:
            cmd += " %s print.timestamp=true" % formatter_property_arg

        if self.print_key:
            cmd += " %s print.key=true" % formatter_property_arg

        if self.print_partition:
            cmd += " %s print.partition=true" % formatter_property_arg

        cmd += " --formatter org.apache.kafka.tools.consumer.LoggingMessageFormatter"

        if self.enable_systest_events:
            cmd += " --enable-systest-events"

        command_property_arg = "--command-property" if version.supports_command_property() else "--consumer-property"
        if self.share_consumer_properties is not None:
            for k, v in self.share_consumer_properties.items():
                cmd += " %s %s=%s" % (command_property_arg, k, v)

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s &" % args
        return cmd

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % ConsoleShareConsumer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload config file
        self.logger.info("share_console_consumer.properties:")

        self.security_config = self.kafka.security_config.client_config(node=node,
                                                                        jaas_override_variables=self.jaas_override_variables)
        self.security_config.setup_node(node)

        if self.client_prop_file_override:
            prop_file = self.client_prop_file_override
        else:
            prop_file = self.prop_file(node)

        self.logger.info(prop_file)
        node.account.create_file(ConsoleShareConsumer.CONFIG_FILE, prop_file)

        # Create and upload log properties
        log_config = self.render(get_log4j_config_for_tools(node), log_file=ConsoleShareConsumer.LOG_FILE)
        node.account.create_file(get_log4j_config_for_tools(node), log_config)

        # Run and capture output
        cmd = self.start_cmd(node)
        self.logger.debug("Console share consumer %d command: %s", idx, cmd)

        share_consumer_output = node.account.ssh_capture(cmd, allow_fail=False)

        with self.lock:
            self.logger.debug("collecting following jmx objects: %s", self.jmx_object_names)
            self.start_jmx_tool(idx, node)

        for line in share_consumer_output:
            msg = line.strip()
            if msg == "shutdown_complete":
                if node in self.clean_shutdown_nodes:
                    raise Exception("Unexpected shutdown event from share consumer, already shutdown. Share consumer index: %d" % idx)
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
        self.logger.info("%s Stopping node %s" % (self.__class__.__name__, str(node.account)))
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=True, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        JmxMixin.clean_node(self, node)
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s" % ConsoleShareConsumer.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def java_class_name(self):
        return "ConsoleShareConsumer"

    def has_log_message(self, node, message):
        try:
            node.account.ssh("grep '%s' %s" % (message, ConsoleShareConsumer.LOG_FILE))
        except RemoteCommandError:
            return False
        return True
