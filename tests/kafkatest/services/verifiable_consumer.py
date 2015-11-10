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

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.services.kafka.directory import kafka_dir, KAFKA_TRUNK
from kafkatest.services.kafka.version import TRUNK
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.kafka import TopicPartition

from collections import namedtuple
import json
import os
import subprocess
import time
import signal

class VerifiableConsumer(BackgroundThreadService):
    PERSISTENT_ROOT = "/mnt/verifiable_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_consumer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "verifiable_consumer.properties")

    logs = {
        "verifiable_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "verifiable_consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "verifiable_consumer_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, kafka, topic, group_id,
                 max_messages=-1, session_timeout=30000, version=TRUNK):
        super(VerifiableConsumer, self).__init__(context, num_nodes)
        self.log_level = "TRACE"
        
        self.kafka = kafka
        self.topic = topic
        self.group_id = group_id
        self.max_messages = max_messages
        self.session_timeout = session_timeout

        self.assignment = {}
        self.joined = set()
        self.total_records = 0
        self.consumed_positions = {}
        self.committed_offsets = {}
        self.revoked_count = 0
        self.assigned_count = 0

        for node in self.nodes:
            node.version = version

        self.prop_file = ""
        self.security_config = kafka.security_config.client_config(self.prop_file)
        self.prop_file += str(self.security_config)

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % VerifiableConsumer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=VerifiableConsumer.LOG_FILE)
        node.account.create_file(VerifiableConsumer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        self.logger.info("verifiable_consumer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableConsumer.CONFIG_FILE, self.prop_file)
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            event = self.try_parse_json(line.strip())
            if event is not None:
                with self.lock:
                    name = event["name"]
                    if name == "shutdown_complete":
                        self._handle_shutdown_complete(node)
                    if name == "offsets_committed":
                        self._handle_offsets_committed(node, event)
                    elif name == "records_consumed":
                        self._handle_records_consumed(node, event)
                    elif name == "partitions_revoked":
                        self._handle_partitions_revoked(node, event)
                    elif name == "partitions_assigned":
                        self._handle_partitions_assigned(node, event)

    def _handle_shutdown_complete(self, node):
        if node in self.joined:
            self.joined.remove(node)

    def _handle_offsets_committed(self, node, event):
        if event["success"]:
            for offset_commit in event["offsets"]:
                topic = offset_commit["topic"]
                partition = offset_commit["partition"]
                tp = TopicPartition(topic, partition)
                self.committed_offsets[tp] = offset_commit["offset"]

    def _handle_records_consumed(self, node, event):
        for topic_partition in event["partitions"]:
            topic = topic_partition["topic"]
            partition = topic_partition["partition"]
            tp = TopicPartition(topic, partition)
            self.consumed_positions[tp] = topic_partition["maxOffset"] + 1
        self.total_records += event["count"]

    def _handle_partitions_revoked(self, node, event):
        self.revoked_count += 1
        self.assignment[node] = []
        if node in self.joined:
            self.joined.remove(node)

    def _handle_partitions_assigned(self, node, event):
        self.assigned_count += 1
        self.joined.add(node)
        assignment =[]
        for topic_partition in event["partitions"]:
            topic = topic_partition["topic"]
            partition = topic_partition["partition"]
            assignment.append(TopicPartition(topic, partition))
        self.assignment[node] = assignment

    def start_cmd(self, node):
        cmd = ""
        cmd += "export LOG_DIR=%s;" % VerifiableConsumer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % VerifiableConsumer.LOG4J_CONFIG
        cmd += "/opt/" + kafka_dir(node) + "/bin/kafka-run-class.sh org.apache.kafka.tools.VerifiableConsumer" \
              " --group-id %s --topic %s --broker-list %s --session-timeout %s" % \
              (self.group_id, self.topic, self.kafka.bootstrap_servers(), self.session_timeout)
        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)

        cmd += " --consumer.config %s" % VerifiableConsumer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableConsumer.STDOUT_CAPTURE, VerifiableConsumer.STDOUT_CAPTURE)
        print(cmd)
        return cmd

    def pids(self, node):
        try:
            cmd = "jps | grep -i VerifiableConsumer | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            return json.loads(string)
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        if clean_shutdown:
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

        if not clean_shutdown:
            self._handle_shutdown_complete(node)

    def stop_node(self, node, clean_shutdown=True, allow_fail=False):
        self.kill_node(node, clean_shutdown, allow_fail)
        
        if self.worker_threads is None:
            return

        # block until the corresponding thread exits
        if len(self.worker_threads) >= self.idx(node):
            # Need to guard this because stop is preemptively called before the worker threads are added and started
            self.worker_threads[self.idx(node) - 1].join()

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def current_assignment(self):
        with self.lock:
            return self.assignment

    def position(self, tp):
        with self.lock:
            return self.consumed_positions[tp]

    def owner(self, tp):
        with self.lock:
            for node, assignment in self.assignment.iteritems():
                if tp in assignment:
                    return node
            return None

    def committed(self, tp):
        with self.lock:
            return self.committed_offsets[tp]

