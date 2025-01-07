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
import json
import signal

from ducktape.utils.util import wait_until
from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.services.kafka.util import get_log4j_config_param, get_log4j_config_for_tools


class TransactionalMessageCopier(KafkaPathResolverMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.TransactionalMessageCopier for
    use in system testing.
    """
    PERSISTENT_ROOT = "/mnt/transactional_message_copier"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_message_copier.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_message_copier.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "transactional_message_copier.log")

    logs = {
        "transactional_message_copier_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "transactional_message_copier_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "transactional_message_copier_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, transactional_id, consumer_group,
                 input_topic, input_partition, output_topic, max_messages=-1,
                 transaction_size=1000, transaction_timeout=None, enable_random_aborts=True,
                 use_group_metadata=False, group_mode=False):
        super(TransactionalMessageCopier, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.transactional_id = transactional_id
        self.consumer_group = consumer_group
        self.transaction_size = transaction_size
        self.transaction_timeout = transaction_timeout
        self.input_topic = input_topic
        self.input_partition = input_partition
        self.output_topic = output_topic
        self.max_messages = max_messages
        self.message_copy_finished = False
        self.consumed = -1
        self.remaining = -1
        self.stop_timeout_sec = 60
        self.enable_random_aborts = enable_random_aborts
        self.use_group_metadata = use_group_metadata
        self.group_mode = group_mode
        self.loggers = {
            "org.apache.kafka.clients.producer": "TRACE",
            "org.apache.kafka.clients.consumer": "TRACE"
        }

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % TransactionalMessageCopier.PERSISTENT_ROOT,
                         allow_fail=False)
        # Create and upload log properties
        log_config = self.render(get_log4j_config_for_tools(node),
                                 log_file=TransactionalMessageCopier.LOG_FILE)
        node.account.create_file(get_log4j_config_for_tools(node), log_config)
        # Configure security
        self.security_config = self.kafka.security_config.client_config(node=node)
        self.security_config.setup_node(node)
        cmd = self.start_cmd(node, idx)
        self.logger.debug("TransactionalMessageCopier %d command: %s" % (idx, cmd))
        try:
            for line in node.account.ssh_capture(cmd):
                line = line.strip()
                data = self.try_parse_json(line)
                if data is not None:
                    with self.lock:
                        self.remaining = int(data["remaining"])
                        self.consumed = int(data["consumed"])
                        self.logger.info("%s: consumed %d, remaining %d" %
                                         (self.transactional_id, self.consumed, self.remaining))
                        if data["stage"] == "ShutdownComplete":
                           if self.remaining == 0:
                                # We are only finished if the remaining
                                # messages at the time of shutdown is 0.
                                #
                                # Otherwise a clean shutdown would still print
                                # a 'shutdown complete' messages even though
                                # there are unprocessed messages, causing
                                # tests to fail.
                                self.logger.info("%s : Finished message copy" % self.transactional_id)
                                self.message_copy_finished = True
                           else:
                               self.logger.info("%s : Shut down without finishing message copy." %\
                                                self.transactional_id)
        except RemoteCommandError as e:
            self.logger.debug("Got exception while reading output from copier, \
                              probably because it was SIGKILL'd (exit code 137): %s" % str(e))

    def start_cmd(self, node, idx):
        cmd  = "export LOG_DIR=%s;" % TransactionalMessageCopier.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"%s%s\"; " % (get_log4j_config_param(node), get_log4j_config_for_tools(node))
        cmd += self.path.script("kafka-run-class.sh", node) + " org.apache.kafka.tools." + "TransactionalMessageCopier"
        cmd += " --broker-list %s" % self.kafka.bootstrap_servers(self.security_config.security_protocol)
        cmd += " --transactional-id %s" % self.transactional_id
        cmd += " --consumer-group %s" % self.consumer_group
        cmd += " --input-topic %s" % self.input_topic
        cmd += " --output-topic %s" % self.output_topic
        cmd += " --input-partition %s" % str(self.input_partition)
        cmd += " --transaction-size %s" % str(self.transaction_size)

        if self.transaction_timeout is not None:
            cmd += " --transaction-timeout %s" % str(self.transaction_timeout)

        if self.enable_random_aborts:
            cmd += " --enable-random-aborts"

        if self.use_group_metadata:
            cmd += " --use-group-metadata"

        if self.group_mode:
            cmd += " --group-mode"

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        cmd += " 2>> %s | tee -a %s &" % (TransactionalMessageCopier.STDERR_CAPTURE, TransactionalMessageCopier.STDOUT_CAPTURE)

        return cmd

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def pids(self, node):
        try:
            cmd = "jps | grep -i TransactionalMessageCopier | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            self.logger.error("Could not list pids: %s" % str(e))
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def start_node(self, node):
        BackgroundThreadService.start_node(self, node)
        wait_until(lambda: self.alive(node), timeout_sec=60, err_msg="Node %s: Message Copier failed to start" % str(node.account))

    def kill_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL
        for pid in pids:
            node.account.signal(pid, sig)
        wait_until(lambda: not self.pids(node), timeout_sec=60, err_msg="Node %s: Message Copier failed to stop" % str(node.account))

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown)
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
            (str(node.account), str(self.stop_timeout_sec))

    def restart(self, clean_shutdown):
        if self.is_done:
            return
        node = self.nodes[0]
        with self.lock:
            self.consumed = -1
            self.remaining = -1
        self.stop_node(node, clean_shutdown)
        self.start_node(node)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None

    @property
    def is_done(self):
        return self.message_copy_finished

    def progress_percent(self):
        with self.lock:
            if self.remaining < 0:
                return 0
            if self.consumed + self.remaining == 0:
                return 100
            return (float(self.consumed)/float(self.consumed + self.remaining)) * 100
