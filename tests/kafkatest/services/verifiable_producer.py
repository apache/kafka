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

import json
import os
import signal
import subprocess
import time

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin, TOOLS_JAR_NAME, TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.utils import is_int, is_int_with_prefix
from kafkatest.version import TRUNK, LATEST_0_8_2


class VerifiableProducer(KafkaPathResolverMixin, BackgroundThreadService):
    PERSISTENT_ROOT = "/mnt/verifiable_producer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_producer.stdout")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_producer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "verifiable_producer.properties")

    logs = {
        "verifiable_producer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "verifiable_producer_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, kafka, topic, max_messages=-1, throughput=100000,
                 message_validator=is_int, compression_types=None, version=TRUNK, acks=None,
                 stop_timeout_sec=150):
        """
        :param max_messages is a number of messages to be produced per producer
        :param message_validator checks for an expected format of messages produced. There are
        currently two:
               * is_int is an integer format; this is default and expected to be used if
               num_nodes = 1
               * is_int_with_prefix recommended if num_nodes > 1, because otherwise each producer
               will produce exactly same messages, and validation may miss missing messages.
        :param compression_types: If None, all producers will not use compression; or a list of
        compression types, one per producer (could be "none").
        """
        super(VerifiableProducer, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.max_messages = max_messages
        self.throughput = throughput
        self.message_validator = message_validator
        self.compression_types = compression_types
        if self.compression_types is not None:
            assert len(self.compression_types) == num_nodes, "Specify one compression type per node"

        for node in self.nodes:
            node.version = version
        self.acked_values = []
        self.not_acked_values = []
        self.produced_count = {}
        self.clean_shutdown_nodes = set()
        self.acks = acks
        self.stop_timeout_sec = stop_timeout_sec

    @property
    def security_config(self):
        return self.kafka.security_config.client_config()

    def prop_file(self, node):
        idx = self.idx(node)
        prop_file = str(self.security_config)
        if self.compression_types is not None:
            compression_index = idx - 1
            self.logger.info("VerifiableProducer (index = %d) will use compression type = %s", idx,
                             self.compression_types[compression_index])
            prop_file += "\ncompression.type=%s\n" % self.compression_types[compression_index]
        return prop_file

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % VerifiableProducer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=VerifiableProducer.LOG_FILE)
        node.account.create_file(VerifiableProducer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        producer_prop_file = self.prop_file(node)
        if self.acks is not None:
            self.logger.info("VerifiableProducer (index = %d) will use acks = %s", idx, self.acks)
            producer_prop_file += "\nacks=%s\n" % self.acks
        self.logger.info("verifiable_producer.properties:")
        self.logger.info(producer_prop_file)
        node.account.create_file(VerifiableProducer.CONFIG_FILE, producer_prop_file)
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node, idx)
        self.logger.debug("VerifiableProducer %d command: %s" % (idx, cmd))

        self.produced_count[idx] = 0
        last_produced_time = time.time()
        prev_msg = None
        for line in node.account.ssh_capture(cmd):
            line = line.strip()

            data = self.try_parse_json(line)
            if data is not None:

                with self.lock:
                    if data["name"] == "producer_send_error":
                        data["node"] = idx
                        self.not_acked_values.append(self.message_validator(data["value"]))
                        self.produced_count[idx] += 1

                    elif data["name"] == "producer_send_success":
                        self.acked_values.append(self.message_validator(data["value"]))
                        self.produced_count[idx] += 1

                        # Log information if there is a large gap between successively acknowledged messages
                        t = time.time()
                        time_delta_sec = t - last_produced_time
                        if time_delta_sec > 2 and prev_msg is not None:
                            self.logger.debug(
                                "Time delta between successively acked messages is large: " +
                                "delta_t_sec: %s, prev_message: %s, current_message: %s" % (str(time_delta_sec), str(prev_msg), str(data)))

                        last_produced_time = t
                        prev_msg = data

                    elif data["name"] == "shutdown_complete":
                        if node in self.clean_shutdown_nodes:
                            raise Exception("Unexpected shutdown event from producer, already shutdown. Producer index: %d" % idx)
                        self.clean_shutdown_nodes.add(node)

    def start_cmd(self, node, idx):
        cmd = ""
        if node.version <= LATEST_0_8_2:
            # 0.8.2.X releases do not have VerifiableProducer.java, so cheat and add
            # the tools jar from trunk to the classpath
            tools_jar = self.path.jar(TOOLS_JAR_NAME, TRUNK)
            tools_dependant_libs_jar = self.path.jar(TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME, TRUNK)

            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % tools_jar
            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % tools_dependant_libs_jar
            cmd += "export CLASSPATH; "

        cmd += "export LOG_DIR=%s;" % VerifiableProducer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % VerifiableProducer.LOG4J_CONFIG
        cmd += " " + self.path.script("kafka-run-class.sh", node)
        cmd += " org.apache.kafka.tools.VerifiableProducer"
        cmd += " --topic %s --broker-list %s" % (self.topic, self.kafka.bootstrap_servers(self.security_config.security_protocol))
        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        if self.throughput > 0:
            cmd += " --throughput %s" % str(self.throughput)
        if self.message_validator == is_int_with_prefix:
            cmd += " --value-prefix %s" % str(idx)
        if self.acks is not None:
            cmd += " --acks %s\n" % str(self.acks)

        cmd += " --producer.config %s" % VerifiableProducer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableProducer.STDOUT_CAPTURE, VerifiableProducer.STDOUT_CAPTURE)
        return cmd

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        if clean_shutdown:
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

    def pids(self, node):
        try:
            cmd = "jps | grep -i VerifiableProducer | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    @property
    def acked(self):
        with self.lock:
            return self.acked_values

    @property
    def not_acked(self):
        with self.lock:
            return self.not_acked_values

    @property
    def num_acked(self):
        with self.lock:
            return len(self.acked_values)

    @property
    def num_not_acked(self):
        with self.lock:
            return len(self.not_acked_values)

    def each_produced_at_least(self, count):
        with self.lock:
            for idx in range(1, self.num_nodes + 1):
                if self.produced_count.get(idx) is None or self.produced_count[idx] < count:
                    return False
            return True

    def stop_node(self, node):
        self.kill_node(node, clean_shutdown=True, allow_fail=False)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None
