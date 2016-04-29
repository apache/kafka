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

from ducktape.utils.util import wait_until
from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.services.kafka.directory import kafka_dir
from kafkatest.services.kafka.version import TRUNK, LATEST_0_8_2, LATEST_0_9
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.security.security_config import SecurityConfig

import itertools
import os
import subprocess


"""
0.8.2.1 ConsoleConsumer options

The console consumer is a tool that reads data from Kafka and outputs it to standard output.
Option                                  Description
------                                  -----------
--blacklist <blacklist>                 Blacklist of topics to exclude from
                                          consumption.
--consumer.config <config file>         Consumer config properties file.
--csv-reporter-enabled                  If set, the CSV metrics reporter will
                                          be enabled
--delete-consumer-offsets               If specified, the consumer path in
                                          zookeeper is deleted when starting up
--formatter <class>                     The name of a class to use for
                                          formatting kafka messages for
                                          display. (default: kafka.tools.
                                          DefaultMessageFormatter)
--from-beginning                        If the consumer does not already have
                                          an established offset to consume
                                          from, start with the earliest
                                          message present in the log rather
                                          than the latest message.
--max-messages <Integer: num_messages>  The maximum number of messages to
                                          consume before exiting. If not set,
                                          consumption is continual.
--metrics-dir <metrics dictory>         If csv-reporter-enable is set, and
                                          this parameter isset, the csv
                                          metrics will be outputed here
--property <prop>
--skip-message-on-error                 If there is an error when processing a
                                          message, skip it instead of halt.
--topic <topic>                         The topic id to consume on.
--whitelist <whitelist>                 Whitelist of topics to include for
                                          consumption.
--zookeeper <urls>                      REQUIRED: The connection string for
                                          the zookeeper connection in the form
                                          host:port. Multiple URLS can be
                                          given to allow fail-over.
"""


class ConsoleConsumer(JmxMixin, BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/console_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "console_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "console_consumer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "console_consumer.properties")

    logs = {
        "consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "consumer_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, group_id="test-consumer-group", new_consumer=False,
                 message_validator=None, from_beginning=True, consumer_timeout_ms=None, version=TRUNK,
                 client_id="console-consumer", print_key=False, jmx_object_names=None, jmx_attributes=[]):
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
            print_key                   if True, print each message's key in addition to its value
        """
        JmxMixin.__init__(self, num_nodes, jmx_object_names, jmx_attributes)
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
        self.security_config = self.kafka.security_config.client_config(prop_file)

        prop_file += str(self.security_config)
        return prop_file

    def start_cmd(self, node):
        """Return the start command appropriate for the given node."""
        args = self.args.copy()
        args['zk_connect'] = self.kafka.zk.connect_setting()
        args['stdout'] = ConsoleConsumer.STDOUT_CAPTURE
        args['stderr'] = ConsoleConsumer.STDERR_CAPTURE
        args['log_dir'] = ConsoleConsumer.LOG_DIR
        args['log4j_config'] = ConsoleConsumer.LOG4J_CONFIG
        args['config_file'] = ConsoleConsumer.CONFIG_FILE
        args['stdout'] = ConsoleConsumer.STDOUT_CAPTURE
        args['jmx_port'] = self.jmx_port
        args['kafka_dir'] = kafka_dir(node)
        args['broker_list'] = self.kafka.bootstrap_servers(self.security_config.security_protocol)
        args['kafka_opts'] = self.security_config.kafka_opts

        cmd = "export JMX_PORT=%(jmx_port)s; " \
              "export LOG_DIR=%(log_dir)s; " \
              "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j_config)s\"; " \
              "export KAFKA_OPTS=%(kafka_opts)s; " \
              "/opt/%(kafka_dir)s/bin/kafka-console-consumer.sh " \
              "--topic %(topic)s --consumer.config %(config_file)s" % args

        if self.new_consumer:
            cmd += " --new-consumer --bootstrap-server %(broker_list)s" % args
        else:
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

        # LoggingMessageFormatter was introduced after 0.9
        if node.version > LATEST_0_9:
            cmd+=" --formatter kafka.tools.LoggingMessageFormatter"

        cmd += " --enable-systest-events"
        cmd += " 2>> %(stderr)s | tee -a %(stdout)s &" % args
        return cmd

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i console_consumer | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
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
        self.security_config.setup_node(node)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=ConsoleConsumer.LOG_FILE)
        node.account.create_file(ConsoleConsumer.LOG4J_CONFIG, log_config)

        # Run and capture output
        cmd = self.start_cmd(node)
        self.logger.debug("Console consumer %d command: %s", idx, cmd)

        consumer_output = node.account.ssh_capture(cmd, allow_fail=False)
        first_line = next(consumer_output, None)

        if first_line is not None:
            self.start_jmx_tool(idx, node)

            for line in itertools.chain([first_line], consumer_output):
                msg = line.strip()
                if msg == "shutdown_complete":
                    if node in self.clean_shutdown_nodes:
                        raise Exception("Unexpected shutdown event from consumer, already shutdown. Consumer index: %d" % idx)
                    self.clean_shutdown_nodes.add(node)
                else:
                    if self.message_validator is not None:
                        msg = self.message_validator(msg)
                    if msg is not None:
                        self.messages_consumed[idx].append(msg)

            self.read_jmx_output(idx, node)

    def start_node(self, node):
        BackgroundThreadService.start_node(self, node)

    def stop_node(self, node):
        node.account.kill_process("console_consumer", allow_fail=True)
        wait_until(lambda: not self.alive(node), timeout_sec=10, backoff_sec=.2,
                   err_msg="Timed out waiting for consumer to stop.")

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        JmxMixin.clean_node(self, node)
        node.account.kill_process("java", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s" % ConsoleConsumer.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)
