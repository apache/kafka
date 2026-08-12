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

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import (
    INMEMORY_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS,
    StreamsTopologyDescriptionPluginService,
)


class StreamsTopologyDescriptionPluginTest(Test):

    PUSH_REQUESTED_LOG = "Broker requested topology description push"
    PUSH_SENDING_LOG = "Sending topology description for group"
    PUSH_SUCCESS_LOG = "Topology description pushed successfully"
    PUSH_FAILED_LOG = "Topology description push failed with non-retriable exception"
    STREAMS_RUNNING_LOG = "State transition from REBALANCING to RUNNING"
    BROKER_SOLICITED_LOG = "Requested topology description push at topology epoch"
    BROKER_LOG_FILE = "%s/server.log" % KafkaService.OPERATIONAL_LOG_INFO_DIR

    SOURCE_TOPIC = "topologyDescriptionPluginSource"
    SINK_TOPIC = "topologyDescriptionPluginSink"

    def __init__(self, test_context):
        super(StreamsTopologyDescriptionPluginTest, self).__init__(test_context=test_context)
        self.topics = {
            self.SOURCE_TOPIC: {"partitions": 1, "replication-factor": 1},
            self.SINK_TOPIC: {"partitions": 1, "replication-factor": 1},
        }

    def setup_kafka(self, plugin_enabled):
        server_prop_overrides = [
            ["group.streams.min.session.timeout.ms", "10000"],
            ["group.streams.session.timeout.ms", "10000"],
        ]
        if plugin_enabled:
            server_prop_overrides.append(
                ["group.streams.topology.description.plugin.class", INMEMORY_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS])
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=1,
            zk=None,
            topics=self.topics,
            use_streams_groups=True,
            server_prop_overrides=server_prop_overrides,
        )
        self.kafka.start()
        self.kafka.run_features_command("upgrade", "streams.version", 1)

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_available_with_plugin(self, metadata_quorum):
        """
        Test the situation when the broker has the topology description plugin configured
        and the client pushes by default. The broker should solicit a push and the client
        should complete it successfully.
        """
        self.setup_kafka(plugin_enabled=True)
        processor = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.start()

            monitor.wait_until(self.PUSH_SUCCESS_LOG,
                               timeout_sec=120,
                               err_msg="Streams client did not log a successful topology description push")
        processor.stop()

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_not_stored_when_client_opts_out(self, metadata_quorum):
        """
        Test the situation when the broker has the plugin configured and solicits a
        topology description push on the heartbeat response (sets
        topologyDescriptionRequired=true), but the client has
        topology.description.push.enabled=false. StreamThread never builds a wire
        description, so the StreamsGroupHeartbeatRequestManager suppresses the
        "Broker requested topology description push" log message. never sends the push RPC
        and the plugin's setTopology is never invoked.
        """
        self.setup_kafka(plugin_enabled=True)
        processor = StreamsTopologyDescriptionPluginService(
            self.test_context, self.kafka, topology_description_push_enabled=False)
        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.start()
            monitor.wait_until(self.STREAMS_RUNNING_LOG,
                               timeout_sec=60,
                               err_msg="Never saw 'REBALANCING -> RUNNING' message " + str(processor.node.account))

        broker_node = self.kafka.nodes[0]
        solicited = broker_node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.BROKER_SOLICITED_LOG, self.BROKER_LOG_FILE),
            allow_fail=False)
        assert int(next(solicited).strip()) > 0, \
            "Broker never solicited a topology push despite the plugin being configured"

        acknowledged = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_REQUESTED_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(acknowledged).strip()) == 0, \
            "Client acknowledged the broker's solicitation despite topology.description.push.enabled=false"

        sent = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SENDING_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(sent).strip()) == 0, \
            "Client sent a topology description despite topology.description.push.enabled=false"

        pushed = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SUCCESS_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(pushed).strip()) == 0, \
            "Client logged a successful push despite topology.description.push.enabled=false"
        processor.stop()

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_not_stored_without_plugin(self, metadata_quorum):
        """
        Test the situation when no topology description plugin is configured on the broker.
        The broker never sets topologyDescriptionRequired=true, so the client is never
        asked to push.
        """
        self.setup_kafka(plugin_enabled=False)

        processor = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.start()
            monitor.wait_until(self.STREAMS_RUNNING_LOG,
                               timeout_sec=60,
                               err_msg="Never saw 'REBALANCING -> RUNNING' message " + str(processor.node.account))

        solicited = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_REQUESTED_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(solicited).strip()) == 0, \
            "Broker solicited a topology push despite no plugin being configured on the broker"

        sent = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SENDING_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(sent).strip()) == 0, \
            "Client sent a topology description despite no plugin being configured on the broker"

        pushed = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SUCCESS_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(pushed).strip()) == 0, \
            "Client logged a successful push despite no plugin being configured on the broker"
        processor.stop()

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_not_resolicited_after_client_restart(self, metadata_quorum):
        """
        Test the situation when the client restarts after already having pushed its
        topology description successfully. The broker still has storedDescriptionTopologyEpoch
        matching currentTopologyEpoch, so it must not solicit a second push.
        """
        self.setup_kafka(plugin_enabled=True)
        processor = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.start()
            monitor.wait_until(self.PUSH_SUCCESS_LOG,
                               timeout_sec=120,
                               err_msg="Streams client did not log a successful topology description push")

        broker_node = self.kafka.nodes[0]
        solicited_before = broker_node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.BROKER_SOLICITED_LOG, self.BROKER_LOG_FILE),
            allow_fail=False)
        solicited_before_count = int(next(solicited_before).strip())
        assert solicited_before_count > 0, \
            "Broker never solicited the initial topology push despite the plugin being configured"

        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.restart()
            monitor.wait_until(self.STREAMS_RUNNING_LOG,
                               timeout_sec=60,
                               err_msg="Never saw 'REBALANCING -> RUNNING' message after client restart " + str(processor.node.account))

        solicited_after = broker_node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.BROKER_SOLICITED_LOG, self.BROKER_LOG_FILE),
            allow_fail=False)
        assert int(next(solicited_after).strip()) == solicited_before_count, \
            "Broker re-solicited a topology push after a client restart despite an already-stored, matching-epoch description"

        pushed = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SUCCESS_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(pushed).strip()) == 1, \
            "Client pushed a topology description again after restart despite the broker not soliciting"
        processor.stop()

    @cluster(num_nodes=3)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_only_one_member_pushes(self, metadata_quorum):
        """
        Test the situation when two members of the same streams group start up together.
        StreamsGroupTopologyDescriptionManager.armIfNotActive must prevent every member
        from pushing the same description; only one member's push should succeed,
        regardless of which member wins the race.
        """
        self.setup_kafka(plugin_enabled=True)
        processor1 = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        processor2 = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        processor1.start()
        processor2.start()

        def total_push_successes():
            pushed1 = processor1.node.account.ssh_capture(
                "grep -c '%s' %s || true" % (self.PUSH_SUCCESS_LOG, processor1.LOG_FILE),
                allow_fail=False)
            pushed2 = processor2.node.account.ssh_capture(
                "grep -c '%s' %s || true" % (self.PUSH_SUCCESS_LOG, processor2.LOG_FILE),
                allow_fail=False)
            return int(next(pushed1).strip()) + int(next(pushed2).strip())

        def total_push_failures():
            failed1 = processor1.node.account.ssh_capture(
                "grep -c '%s' %s || true" % (self.PUSH_FAILED_LOG, processor1.LOG_FILE),
                allow_fail=False)
            failed2 = processor2.node.account.ssh_capture(
                "grep -c '%s' %s || true" % (self.PUSH_FAILED_LOG, processor2.LOG_FILE),
                allow_fail=False)
            return int(next(failed1).strip()) + int(next(failed2).strip())

        wait_until(lambda: total_push_successes() >= 1,
                   timeout_sec=120,
                   err_msg=lambda: "Neither streams client logged a successful topology description push"
                                   + (" (a non-retriable push failure was logged instead, see client logs)"
                                      if total_push_failures() > 0 else ""))
        assert total_push_failures() == 0, \
            "A member logged a non-retriable push failure despite a push having succeeded"
        assert total_push_successes() == 1, \
            "Expected exactly one member to push the topology description successfully"

        sent1 = processor1.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SENDING_LOG, processor1.LOG_FILE),
            allow_fail=False)
        sent2 = processor2.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_SENDING_LOG, processor2.LOG_FILE),
            allow_fail=False)
        total_sent = int(next(sent1).strip()) + int(next(sent2).strip())
        assert total_sent == 1, \
            "Expected exactly one member to send a topology description, got %d" % total_sent

        processor1.stop()
        processor2.stop()

    @cluster(num_nodes=3)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_resolicited_after_group_delete_and_recreate(self, metadata_quorum):
        """
        Test the situation when a streams group is deleted after a successful push, then a
        new client joins under the same application.id. GroupCoordinatorShard.
        finalizeStoredDescriptionTopologyEpochAfterDelete clears the deleted group's stored
        epoch and back-off state, so the new incarnation must be freshly solicited rather
        than inheriting the "already stored" state left behind by the deleted group.
        """
        self.setup_kafka(plugin_enabled=True)
        group_id = "kafka-streams-system-test-topology-description-plugin"
        processor = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.start()
            monitor.wait_until(self.PUSH_SUCCESS_LOG,
                               timeout_sec=120,
                               err_msg="Streams client did not log a successful topology description push")

        processor.stop()

        def group_deleted():
            return "was successful" in self.kafka.delete_streams_group(group_id)

        wait_until(group_deleted, timeout_sec=30, backoff_sec=2,
                   err_msg="kafka-streams-groups.sh --delete never reported success for group " + group_id)

        broker_node = self.kafka.nodes[0]
        new_processor = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        with broker_node.account.monitor_log(self.BROKER_LOG_FILE) as broker_monitor, \
             new_processor.node.account.monitor_log(new_processor.LOG_FILE) as client_monitor:
            new_processor.start()
            broker_monitor.wait_until(self.BROKER_SOLICITED_LOG,
                                      timeout_sec=120,
                                      err_msg="Broker never re-solicited the new_processor group despite the old "
                                              "group having been deleted")
            client_monitor.wait_until(self.PUSH_SUCCESS_LOG,
                                      timeout_sec=120,
                                      err_msg="new_processor group's client never pushed successfully")
        new_processor.stop()
