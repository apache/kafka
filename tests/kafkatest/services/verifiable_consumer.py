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
from kafkatest.services.kafka import TopicPartition, consumer_group
from kafkatest.services.verifiable_client import VerifiableClientMixin
from kafkatest.version import DEV_BRANCH, V_2_3_0, V_2_3_1, V_3_7_0, V_4_0_0


class ConsumerState:
    Started = 1
    Dead = 2
    Rebalancing = 3
    Joined = 4


def _create_partition_from_dict(d):
    topic = d["topic"]
    partition = d["partition"]
    return TopicPartition(topic, partition)


class ConsumerEventHandler(object):

    def __init__(self, node, verify_offsets, idx, state=ConsumerState.Dead,
                 revoked_count=0, assigned_count=0, assignment=None,
                 position=None, committed=None, total_consumed=0):
        self.node = node
        self.verify_offsets = verify_offsets
        self.idx = idx
        self.state = state
        self.revoked_count = revoked_count
        self.assigned_count = assigned_count
        self.assignment = assignment if assignment is not None else []
        self.position = position if position is not None else {}
        self.committed = committed if committed is not None else {}
        self.total_consumed = total_consumed

    def handle_shutdown_complete(self, node=None, logger=None):
        self.state = ConsumerState.Dead
        self.assignment = []
        self.position = {}

        if node is not None and logger is not None:
            logger.debug("Shut down %s" % node.account.hostname)

    def handle_startup_complete(self, node, logger):
        self.state = ConsumerState.Started
        logger.debug("Started %s" % node.account.hostname)

    def handle_offsets_committed(self, event, node, logger):
        if event["success"]:
            for offset_commit in event["offsets"]:
                if offset_commit.get("error", "") != "":
                    logger.debug("%s: Offset commit failed for: %s" % (str(node.account), offset_commit))
                    continue

                tp = _create_partition_from_dict(offset_commit)
                offset = offset_commit["offset"]
                assert tp in self.assignment, \
                    "Committed offsets for partition %s not assigned (current assignment: %s)" % \
                    (str(tp), str(self.assignment))
                assert tp in self.position, "No previous position for %s: %s" % (str(tp), event)
                assert self.position[tp] >= offset, \
                    "The committed offset %d was greater than the current position %d for partition %s" % \
                    (offset, self.position[tp], str(tp))
                self.committed[tp] = offset

        logger.debug("Offsets committed for %s" % node.account.hostname)

    def handle_records_consumed(self, event, logger):
        assert self.state == ConsumerState.Joined, \
            "Consumed records should only be received when joined (current state: %s)" % str(self.state)

        for record_batch in event["partitions"]:
            tp = _create_partition_from_dict(record_batch)
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

    def handle_partitions_revoked(self, event, node, logger):
        self.revoked_count += 1
        self.state = ConsumerState.Rebalancing
        self.position = {}
        logger.debug("All partitions revoked from %s" % node.account.hostname)

    def handle_partitions_assigned(self, event, node, logger):
        self.assigned_count += 1
        self.state = ConsumerState.Joined
        assignment = []
        for topic_partition in event["partitions"]:
            tp = _create_partition_from_dict(topic_partition)
            assignment.append(tp)
        logger.debug("Partitions %s assigned to %s" % (assignment, node.account.hostname))
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

# This needs to be used for cooperative protocol.
class IncrementalAssignmentConsumerEventHandler(ConsumerEventHandler):
    def __init__(self, node, verify_offsets, idx, **kwargs):
        super().__init__(node, verify_offsets, idx, **kwargs)
        
    def handle_partitions_revoked(self, event, node, logger):
        self.revoked_count += 1
        self.state = ConsumerState.Rebalancing
        self.position = {}
        revoked = []

        for topic_partition in event["partitions"]:
            tp = _create_partition_from_dict(topic_partition)
            assert tp in self.assignment, \
                "Topic partition %s cannot be revoked from %s as it was not previously assigned to that consumer" % \
                (tp, node.account.hostname)
            self.assignment.remove(tp)
            revoked.append(tp)

        logger.debug("Partitions %s revoked from %s" % (revoked, node.account.hostname))

    def handle_partitions_assigned(self, event, node, logger):
        self.assigned_count += 1
        self.state = ConsumerState.Joined
        assignment = []
        for topic_partition in event["partitions"]:
            tp = _create_partition_from_dict(topic_partition)
            assignment.append(tp)
        logger.debug("Partitions %s assigned to %s" % (assignment, node.account.hostname))
        self.assignment.extend(assignment)

# This needs to be used for consumer protocol.
class ConsumerProtocolConsumerEventHandler(IncrementalAssignmentConsumerEventHandler):
    def __init__(self, node, verify_offsets, idx, **kwargs):
        super().__init__(node, verify_offsets, idx, **kwargs)

    def handle_partitions_revoked(self, event, node, logger):
        # The handler state is not transitioned to Rebalancing as the records can only be
        # consumed in Joined state (see ConsumerEventHandler.handle_records_consumed).
        # The consumer with consumer protocol should still be able to consume messages during rebalance.
        self.revoked_count += 1
        self.position = {}
        revoked = []

        for topic_partition in event["partitions"]:
            tp = _create_partition_from_dict(topic_partition)
            # tp existing in self.assignment is not guaranteed in the new consumer
            # if it shuts down when revoking partitions for reconciliation.
            if tp in self.assignment:
                self.assignment.remove(tp)
            revoked.append(tp)

        logger.debug("Partitions %s revoked from %s" % (revoked, node.account.hostname))

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
                 static_membership=False, max_messages=-1, session_timeout_sec=0, enable_autocommit=False,
                 assignment_strategy=None, group_protocol=None, group_remote_assignor=None,
                 version=DEV_BRANCH, stop_timeout_sec=30, log_level="INFO", jaas_override_variables=None,
                 on_record_consumed=None, reset_policy="earliest", verify_offsets=True, prop_file=""):
        """
        :param jaas_override_variables: A dict of variables to be used in the jaas.conf template file
        """
        super(VerifiableConsumer, self).__init__(context, num_nodes)
        self.log_level = log_level
        self.kafka = kafka
        self.topic = topic
        self.group_protocol = group_protocol
        self.group_remote_assignor = group_remote_assignor
        self.group_id = group_id
        self.reset_policy = reset_policy
        self.static_membership = static_membership
        self.max_messages = max_messages
        self.session_timeout_sec = session_timeout_sec
        self.enable_autocommit = enable_autocommit
        self.assignment_strategy = assignment_strategy
        self.prop_file = prop_file
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

    def create_event_handler(self, idx, node):
        def create_handler_helper(handler_class, node, idx, existing_handler=None):
            if existing_handler is not None:
                return handler_class(node, self.verify_offsets, idx,
                                     state=existing_handler.state,
                                     revoked_count=existing_handler.revoked_count,
                                     assigned_count=existing_handler.assigned_count,
                                     assignment=existing_handler.assignment,
                                     position=existing_handler.position,
                                     committed=existing_handler.committed,
                                     total_consumed=existing_handler.total_consumed)
            else:
                return handler_class(node, self.verify_offsets, idx)
        existing_handler = self.event_handlers[node] if node in self.event_handlers else None
        if self.is_consumer_group_protocol_enabled():
            return create_handler_helper(ConsumerProtocolConsumerEventHandler, node, idx, existing_handler)
        elif self.is_eager():
            return create_handler_helper(ConsumerEventHandler, node, idx, existing_handler)
        else:
            return create_handler_helper(IncrementalAssignmentConsumerEventHandler, node, idx, existing_handler)

    def _worker(self, idx, node):
        with self.lock:
            self.event_handlers[node] = self.create_event_handler(idx, node)
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
            assert node.version >= V_2_3_0, \
                "Version %s does not support static membership (must be 2.3 or higher)" % str(node.version)
            node.group_instance_id = self.group_id + "-instance-" + str(idx)

        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            event = self.try_parse_json(node, line.strip())
            if event is not None:
                with self.lock:
                    name = event["name"]
                    if name == "shutdown_complete":
                        handler.handle_shutdown_complete(node, self.logger)
                    elif name == "startup_complete":
                        handler.handle_startup_complete(node, self.logger)
                    elif name == "offsets_committed":
                        handler.handle_offsets_committed(event, node, self.logger)
                        self._update_global_committed(event)
                    elif name == "records_consumed":
                        handler.handle_records_consumed(event, self.logger)
                        self._update_global_position(event, node)
                    elif name == "record_data" and self.on_record_consumed:
                        self.on_record_consumed(event, node)
                    elif name == "partitions_revoked":
                        handler.handle_partitions_revoked(event, node, self.logger)
                    elif name == "partitions_assigned":
                        handler.handle_partitions_assigned(event, node, self.logger)
                    else:
                        self.logger.debug("%s: ignoring unknown event: %s" % (str(node.account), event))

    def is_eager(self):
        return self.group_protocol == consumer_group.classic_group_protocol and self.assignment_strategy != "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    
    def _update_global_position(self, consumed_event, node):
        for consumed_partition in consumed_event["partitions"]:
            tp = _create_partition_from_dict(consumed_partition)
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
                tp = _create_partition_from_dict(offset_commit)
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

        if node.group_instance_id:
            cmd += " --group-instance-id %s" % node.group_instance_id
        elif node.version == V_2_3_0 or node.version == V_2_3_1:
            # In 2.3, --group-instance-id was required, but would be left empty
            # if `None` is passed as the argument value
            cmd += " --group-instance-id None"

        # 3.7.0 includes support for KIP-848 which introduced a new implementation of the consumer group protocol.
        # The two implementations use slightly different configuration, hence these arguments are conditional.
        #
        # See the Java class/method VerifiableConsumer.createFromArgs() for how the command line arguments are
        # parsed and used as configuration in the runner.
        if node.version >= V_3_7_0 and self.is_consumer_group_protocol_enabled():
            cmd += " --group-protocol %s" % self.group_protocol

            if self.group_remote_assignor:
                cmd += " --group-remote-assignor %s" % self.group_remote_assignor
        else:
            # Either we're an older consumer version or we're using the old consumer group protocol.
            if self.assignment_strategy:
                cmd += " --assignment-strategy %s" % self.assignment_strategy

        if self.enable_autocommit:
            cmd += " --enable-autocommit "

        if node.version < V_4_0_0:
            cmd += " --broker-list %s" % self.kafka.bootstrap_servers(self.security_config.security_protocol)
        else:
            cmd += " --bootstrap-server %s" % self.kafka.bootstrap_servers(self.security_config.security_protocol)

        cmd += " --reset-policy %s --group-id %s --topic %s" % \
                (self.reset_policy, self.group_id, self.topic)

        if self.session_timeout_sec > 0:
            cmd += " --session-timeout %s" % self.session_timeout_sec*1000

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
            return { handler.node: handler.current_assignment() for handler in self.event_handlers.values() }

    def current_position(self, tp):
        with self.lock:
            if tp in self.global_position:
                return self.global_position[tp]
            else:
                return None

    def owner(self, tp):
        with self.lock:
            for handler in self.event_handlers.values():
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
            return sum(handler.total_consumed for handler in self.event_handlers.values())

    def num_rebalances(self):
        with self.lock:
            return max(handler.assigned_count for handler in self.event_handlers.values())

    def num_revokes_for_alive(self, keep_alive=1):
        with self.lock:
            return max(handler.revoked_count for handler in self.event_handlers.values()
                       if handler.idx <= keep_alive)

    def joined_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.values()
                    if handler.state == ConsumerState.Joined]

    def rebalancing_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.values()
                    if handler.state == ConsumerState.Rebalancing]

    def dead_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.values()
                    if handler.state == ConsumerState.Dead]

    def alive_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.values()
                    if handler.state != ConsumerState.Dead]

    def is_consumer_group_protocol_enabled(self):
        return self.group_protocol and self.group_protocol.lower() == consumer_group.consumer_group_protocol
