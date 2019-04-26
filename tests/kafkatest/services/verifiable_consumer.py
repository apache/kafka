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
from kafkatest.services.verifiable_client import VerifiableClientMixin
from kafkatest.version import DEV_BRANCH


class ConsumerState:
    Started = 1
    Dead = 2
    Rebalancing = 3
    Joined = 4


class ConsumerEventHandler(object):

    def __init__(self, node, verify_offsets, idx):
        self.node = node
        self.idx = idx
        self.state = ConsumerState.Dead
        self.revoked_count = 0
        self.assigned_count = 0
        self.assignment = []
        self.position = {}
        self.committed = {}
        self.total_consumed = 0
        self.verify_offsets = verify_offsets

    def handle_shutdown_complete(self):
        self.state = ConsumerState.Dead
        self.assignment = []
        self.position = {}

    def handle_startup_complete(self):
        self.state = ConsumerState.Started

    def handle_offsets_committed(self, event, node, logger):
        if event["success"]:
            for offset_commit in event["offsets"]:
                if offset_commit.get("error", "") != "":
                    logger.debug("%s: Offset commit failed for: %s" % (str(node.account), offset_commit))
                    continue

                topic = offset_commit["topic"]
                partition = offset_commit["partition"]
                tp = TopicPartition(topic, partition)
                offset = offset_commit["offset"]
                assert tp in self.assignment, \
                    "Committed offsets for partition %s not assigned (current assignment: %s)" % \
                    (str(tp), str(self.assignment))
                assert tp in self.position, "No previous position for %s: %s" % (str(tp), event)
                assert self.position[tp] >= offset, \
                    "The committed offset %d was greater than the current position %d for partition %s" % \
                    (offset, self.position[tp], str(tp))
                self.committed[tp] = offset

    def handle_records_consumed(self, event, logger):
        assert self.state == ConsumerState.Joined, \
            "Consumed records should only be received when joined (current state: %s)" % str(self.state)

        for record_batch in event["partitions"]:
            tp = TopicPartition(topic=record_batch["topic"],
                                partition=record_batch["partition"])
            min_offset = record_batch["minOffset"]
            max_offset = record_batch["maxOffset"]

            assert tp in self.assignment, \
                "Consumed records for partition %s which is not assigned (current assignment: %s)" % \
                (str(tp), str(self.assignment))
            if tp not in self.position or self.position[tp] == min_offset:
                self.position[tp] = max_offset + 1
            else:
                msg = "Consumed from an unexpected offset (%d, %d) for partition %s" % \
                      (self.position.get(tp), min_offset, str(tp))
                if self.verify_offsets:
                    raise AssertionError(msg)
                else:
                    if tp in self.position:
                        self.position[tp] = max_offset + 1
                    logger.warn(msg)
            self.total_consumed += event["count"]

    def handle_partitions_revoked(self, event):
        self.revoked_count += 1
        self.state = ConsumerState.Rebalancing
        self.position = {}

    def handle_partitions_assigned(self, event):
        self.assigned_count += 1
        self.state = ConsumerState.Joined
        assignment = []
        for topic_partition in event["partitions"]:
            topic = topic_partition["topic"]
            partition = topic_partition["partition"]
            assignment.append(TopicPartition(topic, partition))
        self.assignment = assignment

    def handle_kill_process(self, clean_shutdown):
        # if the shutdown was clean, then we expect the explicit
        # shutdown event from the consumer
        if not clean_shutdown:
            self.handle_shutdown_complete()

    def current_assignment(self):
        return list(self.assignment)

    def current_position(self, tp):
        if tp in self.position:
            return self.position[tp]
        else:
            return None

    def last_commit(self, tp):
        if tp in self.committed:
            return self.committed[tp]
        else:
            return None


class VerifiableConsumer(KafkaPathResolverMixin, VerifiableClientMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.VerifiableConsumer for use in
    system testing. 
    
    NOTE: this class should be treated as a PUBLIC API. Downstream users use
    this service both directly and through class extension, so care must be 
    taken to ensure compatibility.
    """

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
                 static_membership=False, max_messages=-1, session_timeout_sec=30, enable_autocommit=False,
                 assignment_strategy="org.apache.kafka.clients.consumer.RangeAssignor",
                 version=DEV_BRANCH, stop_timeout_sec=30, log_level="INFO", jaas_override_variables=None,
                 on_record_consumed=None, reset_policy="earliest", verify_offsets=True):
        """
        :param jaas_override_variables: A dict of variables to be used in the jaas.conf template file
        """
        super(VerifiableConsumer, self).__init__(context, num_nodes)
        self.log_level = log_level
        
        self.kafka = kafka
        self.topic = topic
        self.group_id = group_id
        self.reset_policy = reset_policy
        self.static_membership = static_membership
        self.max_messages = max_messages
        self.session_timeout_sec = session_timeout_sec
        self.enable_autocommit = enable_autocommit
        self.assignment_strategy = assignment_strategy
        self.prop_file = ""
        self.stop_timeout_sec = stop_timeout_sec
        self.on_record_consumed = on_record_consumed
        self.verify_offsets = verify_offsets

        self.event_handlers = {}
        self.global_position = {}
        self.global_committed = {}
        self.jaas_override_variables = jaas_override_variables or {}

        for node in self.nodes:
            node.version = version

    def java_class_name(self):
        return "VerifiableConsumer"

    def _worker(self, idx, node):
        with self.lock:
            if node not in self.event_handlers:
                self.event_handlers[node] = ConsumerEventHandler(node, self.verify_offsets, idx)
            handler = self.event_handlers[node]

        node.account.ssh("mkdir -p %s" % VerifiableConsumer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=VerifiableConsumer.LOG_FILE)
        node.account.create_file(VerifiableConsumer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        self.security_config = self.kafka.security_config.client_config(self.prop_file, node,
                                                                        self.jaas_override_variables)
        self.security_config.setup_node(node)
        self.prop_file += str(self.security_config)
        self.logger.info("verifiable_consumer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableConsumer.CONFIG_FILE, self.prop_file)
        self.security_config.setup_node(node)
        # apply group.instance.id to the node for static membership validation
        node.group_instance_id = None
        if self.static_membership:
            node.group_instance_id = self.group_id + "-instance-" + str(idx)
        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            event = self.try_parse_json(node, line.strip())
            if event is not None:
                with self.lock:
                    name = event["name"]
                    if name == "shutdown_complete":
                        handler.handle_shutdown_complete()
                    elif name == "startup_complete":
                        handler.handle_startup_complete()
                    elif name == "offsets_committed":
                        handler.handle_offsets_committed(event, node, self.logger)
                        self._update_global_committed(event)
                    elif name == "records_consumed":
                        handler.handle_records_consumed(event, self.logger)
                        self._update_global_position(event, node)
                    elif name == "record_data" and self.on_record_consumed:
                        self.on_record_consumed(event, node)
                    elif name == "partitions_revoked":
                        handler.handle_partitions_revoked(event)
                    elif name == "partitions_assigned":
                        handler.handle_partitions_assigned(event)
                    else:
                        self.logger.debug("%s: ignoring unknown event: %s" % (str(node.account), event))

    def _update_global_position(self, consumed_event, node):
        for consumed_partition in consumed_event["partitions"]:
            tp = TopicPartition(consumed_partition["topic"], consumed_partition["partition"])
            if tp in self.global_committed:
                # verify that the position never gets behind the current commit.
                if self.global_committed[tp] > consumed_partition["minOffset"]:
                    msg = "Consumed position %d is behind the current committed offset %d for partition %s" % \
                          (consumed_partition["minOffset"], self.global_committed[tp], str(tp))
                    if self.verify_offsets:
                        raise AssertionError(msg)
                    else:
                        self.logger.warn(msg)

            # the consumer cannot generally guarantee that the position increases monotonically
            # without gaps in the face of hard failures, so we only log a warning when this happens
            if tp in self.global_position and self.global_position[tp] != consumed_partition["minOffset"]:
                self.logger.warn("%s: Expected next consumed offset of %d for partition %s, but instead saw %d" %
                                 (str(node.account), self.global_position[tp], str(tp), consumed_partition["minOffset"]))

            self.global_position[tp] = consumed_partition["maxOffset"] + 1

    def _update_global_committed(self, commit_event):
        if commit_event["success"]:
            for offset_commit in commit_event["offsets"]:
                tp = TopicPartition(offset_commit["topic"], offset_commit["partition"])
                offset = offset_commit["offset"]
                assert self.global_position[tp] >= offset, \
                    "Committed offset %d for partition %s is ahead of the current position %d" % \
                    (offset, str(tp), self.global_position[tp])
                self.global_committed[tp] = offset

    def start_cmd(self, node):
        cmd = ""
        cmd += "export LOG_DIR=%s;" % VerifiableConsumer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % VerifiableConsumer.LOG4J_CONFIG
        cmd += self.impl.exec_cmd(node)
        if self.on_record_consumed:
            cmd += " --verbose"
        cmd += " --reset-policy %s --group-id %s --topic %s --group-instance-id %s --broker-list %s --session-timeout %s --assignment-strategy %s %s" % \
               (self.reset_policy, self.group_id, self.topic, node.group_instance_id, self.kafka.bootstrap_servers(self.security_config.security_protocol),
               self.session_timeout_sec*1000, self.assignment_strategy, "--enable-autocommit" if self.enable_autocommit else "")
               
        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)

        cmd += " --consumer.config %s" % VerifiableConsumer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableConsumer.STDOUT_CAPTURE, VerifiableConsumer.STDOUT_CAPTURE)
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

    def current_assignment(self):
        with self.lock:
            return { handler.node: handler.current_assignment() for handler in self.event_handlers.itervalues() }

    def current_position(self, tp):
        with self.lock:
            if tp in self.global_position:
                return self.global_position[tp]
            else:
                return None

    def owner(self, tp):
        with self.lock:
            for handler in self.event_handlers.itervalues():
                if tp in handler.current_assignment():
                    return handler.node
            return None

    def last_commit(self, tp):
        with self.lock:
            if tp in self.global_committed:
                return self.global_committed[tp]
            else:
                return None

    def total_consumed(self):
        with self.lock:
            return sum(handler.total_consumed for handler in self.event_handlers.itervalues())

    def num_rebalances(self):
        with self.lock:
            return max(handler.assigned_count for handler in self.event_handlers.itervalues())

    def num_revokes_for_alive(self, keep_alive=1):
        with self.lock:
            return max([handler.revoked_count for handler in self.event_handlers.itervalues()
                       if handler.idx <= keep_alive])

    def joined_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state == ConsumerState.Joined]

    def rebalancing_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state == ConsumerState.Rebalancing]

    def dead_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state == ConsumerState.Dead]

    def alive_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state != ConsumerState.Dead]
