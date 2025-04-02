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

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import TopicPartition
from kafkatest.services.kafka.util import get_log4j_config_param, get_log4j_config_for_tools
from kafkatest.services.verifiable_client import VerifiableClientMixin
from kafkatest.version import DEV_BRANCH

class ShareConsumerState:
    Started = 1
    Dead = 2

class ShareConsumerEventHandler(object):

    def __init__(self, node, idx, state=ShareConsumerState.Dead):
        self.node = node
        self.idx = idx
        self.total_consumed = 0
        self.total_acknowledged_successfully = 0
        self.total_acknowledged_failed = 0
        self.consumed_per_partition = {}
        self.acknowledged_per_partition = {}
        self.acknowledged_per_partition_failed = {}
        self.state = state

    def handle_shutdown_complete(self, node=None, logger=None):
        self.state = ShareConsumerState.Dead
        if node is not None and logger is not None:
            logger.debug("Shut down %s" % node.account.hostname)

    def handle_startup_complete(self, node, logger):
        self.state = ShareConsumerState.Started
        logger.debug("Started %s" % node.account.hostname)

    def handle_offsets_acknowledged(self, event, node, logger):
        if event["success"]:
            self.total_acknowledged_successfully += event["count"]
            for share_partition_data in event["partitions"]:
                topic_partition = TopicPartition(share_partition_data["topic"], share_partition_data["partition"])
                self.acknowledged_per_partition[topic_partition] = self.acknowledged_per_partition.get(topic_partition, 0) + share_partition_data["count"]
            logger.debug("Offsets acknowledged for %s" % (node.account.hostname))
        else:
            self.total_acknowledged_failed += event["count"]
            for share_partition_data in event["partitions"]:
                topic_partition = TopicPartition(share_partition_data["topic"], share_partition_data["partition"])
                self.acknowledged_per_partition_failed[topic_partition] = self.acknowledged_per_partition_failed.get(topic_partition, 0) + share_partition_data["count"]
            logger.debug("Offsets acknowledged for %s" % (node.account.hostname))
            logger.debug("Offset acknowledgement failed for: %s" % (node.account.hostname))

    def handle_records_consumed(self, event, node, logger):
        self.total_consumed += event["count"]
        for share_partition_data in event["partitions"]:
            topic_partition = TopicPartition(share_partition_data["topic"], share_partition_data["partition"])
            self.consumed_per_partition[topic_partition] = self.consumed_per_partition.get(topic_partition, 0) + share_partition_data["count"]
        logger.debug("Offsets consumed for %s" % (node.account.hostname))


    def handle_kill_process(self, clean_shutdown):
        # if the shutdown was clean, then we expect the explicit
        # shutdown event from the share consumer
        if not clean_shutdown:
            self.handle_shutdown_complete()

class VerifiableShareConsumer(KafkaPathResolverMixin, VerifiableClientMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.VerifiableShareConsumer for use in
    system testing.

    NOTE: this class should be treated as a PUBLIC API. Downstream users use
    this service both directly and through class extension, so care must be
    taken to ensure compatibility.
    """

    PERSISTENT_ROOT = "/mnt/verifiable_share_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_share_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_share_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_share_consumer.log")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "verifiable_share_consumer.properties")

    logs = {
            "verifiable_share_consumer_stdout": {
                "path": STDOUT_CAPTURE,
                "collect_default": False},
            "verifiable_share_consumer_stderr": {
                "path": STDERR_CAPTURE,
                "collect_default": False},
            "verifiable_share_consumer_log": {
                "path": LOG_FILE,
                "collect_default": True}
            }

    def __init__(self, context, num_nodes, kafka, topic, group_id,
                 max_messages=-1, acknowledgement_mode="auto", offset_reset_strategy="",
                 version=DEV_BRANCH, stop_timeout_sec=60, log_level="INFO", jaas_override_variables=None,
                 on_record_consumed=None):
        """
        :param jaas_override_variables: A dict of variables to be used in the jaas.conf template file
        """
        super(VerifiableShareConsumer, self).__init__(context, num_nodes)
        self.log_level = log_level
        self.kafka = kafka
        self.topic = topic
        self.group_id = group_id
        self.offset_reset_strategy = offset_reset_strategy
        self.max_messages = max_messages
        self.acknowledgement_mode = acknowledgement_mode
        self.prop_file = ""
        self.stop_timeout_sec = stop_timeout_sec
        self.on_record_consumed = on_record_consumed

        self.event_handlers = {}
        self.jaas_override_variables = jaas_override_variables or {}

        self.total_records_consumed = 0
        self.total_records_acknowledged = 0
        self.total_records_acknowledged_failed = 0
        self.consumed_records_offsets = set()
        self.acknowledged_records_offsets = set()
        self.is_offset_reset_strategy_set = False

        for node in self.nodes:
            node.version = version

    def java_class_name(self):
        return "VerifiableShareConsumer"

    def create_event_handler(self, idx, node):
        return ShareConsumerEventHandler(node, idx)

    def _worker(self, idx, node):
        with self.lock:
            self.event_handlers[node] = self.create_event_handler(idx, node)
            handler = self.event_handlers[node]

        node.account.ssh("mkdir -p %s" % VerifiableShareConsumer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render(get_log4j_config_for_tools(node), log_file=VerifiableShareConsumer.LOG_FILE)
        node.account.create_file(get_log4j_config_for_tools(node), log_config)

        # Create and upload config file
        self.security_config = self.kafka.security_config.client_config(self.prop_file, node,
                                                                        self.jaas_override_variables)
        self.security_config.setup_node(node)
        self.prop_file += str(self.security_config)
        self.logger.info("verifiable_share_consumer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableShareConsumer.CONFIG_FILE, self.prop_file)
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableShareConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            event = self.try_parse_json(node, line.strip())
            if event is not None:
                with self.lock:
                    name = event["name"]
                    if name == "shutdown_complete":
                        handler.handle_shutdown_complete(node, self.logger)
                    elif name == "startup_complete":
                        handler.handle_startup_complete(node, self.logger)
                    elif name == "offsets_acknowledged":
                        handler.handle_offsets_acknowledged(event, node, self.logger)
                        self._update_global_acknowledged(event)
                    elif name == "records_consumed":
                        handler.handle_records_consumed(event, node, self.logger)
                        self._update_global_consumed(event)
                    elif name == "record_data" and self.on_record_consumed:
                        self.on_record_consumed(event, node)
                    elif name == "offset_reset_strategy_set":
                        self._on_offset_reset_strategy_set()
                    else:
                        self.logger.debug("%s: ignoring unknown event: %s" % (str(node.account), event))

    def _update_global_acknowledged(self, acknowledge_event):
        if acknowledge_event["success"]:
            self.total_records_acknowledged += acknowledge_event["count"]
        else:
            self.total_records_acknowledged_failed += acknowledge_event["count"]
        for share_partition_data in acknowledge_event["partitions"]:
                tpkey = str(share_partition_data["topic"]) + "-" + str(share_partition_data["partition"])
                for offset in share_partition_data["offsets"]:
                    key = tpkey + "-" + str(offset)
                    if key not in self.acknowledged_records_offsets:
                        self.acknowledged_records_offsets.add(key)

    def _update_global_consumed(self, consumed_event):
        self.total_records_consumed += consumed_event["count"]

        for share_partition_data in consumed_event["partitions"]:
            tpkey = str(share_partition_data["topic"]) + "-" + str(share_partition_data["partition"])
            for offset in share_partition_data["offsets"]:
                key = tpkey + "-" + str(offset)
                if key not in self.consumed_records_offsets:
                    self.consumed_records_offsets.add(key)

    def _on_offset_reset_strategy_set(self):
        self.is_offset_reset_strategy_set = True

    def start_cmd(self, node):
        cmd = ""
        cmd += "export LOG_DIR=%s;" % VerifiableShareConsumer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"%s%s\"; " % (get_log4j_config_param(node), get_log4j_config_for_tools(node))
        cmd += self.impl.exec_cmd(node)
        if self.on_record_consumed:
            cmd += " --verbose"

        cmd += " --acknowledgement-mode %s" % self.acknowledgement_mode

        cmd += " --offset-reset-strategy %s" % self.offset_reset_strategy

        cmd += " --bootstrap-server %s" % self.kafka.bootstrap_servers(self.security_config.security_protocol)

        cmd += " --group-id %s --topic %s" % (self.group_id, self.topic)

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)

        cmd += " --consumer.config %s" % VerifiableShareConsumer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableShareConsumer.STDOUT_CAPTURE, VerifiableShareConsumer.STDOUT_CAPTURE)
        return cmd

    def pids(self, node):
        return self.impl.pids(node)

    def try_parse_json(self, node, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            return json.loads(string)
        except ValueError:
            self.logger.debug("%s: Could not parse as json: %s" % (str(node.account), str(string)))
            return None

    def stop_all(self):
        for node in self.nodes:
            self.stop_node(node)

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        sig = self.impl.kill_signal(clean_shutdown)
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

        with self.lock:
            self.event_handlers[node].handle_kill_process(clean_shutdown)

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown=clean_shutdown)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def total_consumed(self):
        with self.lock:
            return self.total_records_consumed
        
    def total_unique_consumed(self):
        with self.lock:
            return len(self.consumed_records_offsets)

    def total_unique_acknowledged(self):
        with self.lock:
            return len(self.acknowledged_records_offsets)

    def total_acknowledged(self):
        with self.lock:
            return self.total_records_acknowledged + self.total_records_acknowledged_failed
        
    def total_acknowledged_successfully(self):
        with self.lock:
            return self.total_records_acknowledged
        
    def total_failed_acknowledged(self):
        with self.lock:
            return self.total_records_acknowledged_failed

    def total_consumed_for_a_share_consumer(self, node):
        with self.lock:
            return self.event_handlers[node].total_consumed

    def total_acknowledged_for_a_share_consumer(self, node):
        with self.lock:
            return self.event_handlers[node].total_acknowledged_successfully + self.event_handlers[node].total_acknowledged_failed
        
    def total_acknowledged_sucessfully_for_a_share_consumer(self, node):
        with self.lock:
            return self.event_handlers[node].total_acknowledged_successfully
        
    def total_failed_acknowledged_for_a_share_consumer(self, node):
        with self.lock:
            return self.event_handlers[node].total_acknowledged_failed

    def offset_reset_strategy_set(self):
        with self.lock:
            return self.is_offset_reset_strategy_set

    def dead_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.values()
                    if handler.state == ShareConsumerState.Dead]

    def alive_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.values()
                    if handler.state == ShareConsumerState.Started]