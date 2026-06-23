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
from kafkatest.services.streams import StreamsTopologyDescriptionPluginService


PLUGIN_CLASS = "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"
APPLICATION_ID = StreamsTopologyDescriptionPluginService.APPLICATION_ID

TOPICS = {
    "topology-description-source": {"partitions": 1, "replication-factor": 1},
    "topology-description-sink":   {"partitions": 1, "replication-factor": 1},
}

BASE_OVERRIDES = [
    ["group.streams.min.session.timeout.ms", "10000"],
    ["group.streams.session.timeout.ms",     "10000"],
]


def _build_kafka(test_context, plugin_enabled):
    overrides = list(BASE_OVERRIDES)
    if plugin_enabled:
        overrides.insert(0, ["group.streams.topology.description.plugin.class", PLUGIN_CLASS])
    return KafkaService(
        test_context, num_nodes=1, zk=None, topics=TOPICS,
        controller_num_nodes_override=1,
        use_streams_groups=True,
        server_prop_overrides=overrides)


def _describe(kafka):
    return kafka.describe_streams_group_topology(APPLICATION_ID)


class StreamsTopologyDescriptionPluginTest(Test):
    """End-to-end system tests for KIP-1331 when the broker has the topology
    description plugin configured."""

    def __init__(self, test_context):
        super().__init__(test_context=test_context)
        self.kafka = _build_kafka(test_context, plugin_enabled=True)

    def setUp(self):
        self.kafka.start()
        self.kafka.run_features_command("upgrade", "streams.version", 1)

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_pretty_print_via_cli(self, metadata_quorum):
        """A running Streams application pushes its topology and an operator can
        retrieve and pretty-print it via the CLI end-to-end."""
        streams = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        streams.start()
        try:
            def has_topology():
                stdout, exit_code = _describe(self.kafka)
                return exit_code == 0 and "Topologies:" in stdout
            wait_until(has_topology,
                       timeout_sec=120,
                       err_msg="kafka-streams-groups --topology never reported AVAILABLE for %s" % APPLICATION_ID)
            stdout, exit_code = _describe(self.kafka)
            assert exit_code == 0, "expected exit 0, got %d. stdout: %s" % (exit_code, stdout)
            assert "topology-description-source" in stdout, \
                "expected source topic in output. stdout: %s" % stdout
            assert "topology-description-sink" in stdout, \
                "expected sink topic in output. stdout: %s" % stdout
        finally:
            streams.stop_nodes(clean_shutdown=True)

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_disable_feature_on_client(self, metadata_quorum):
        """When the client sets topology.description.push.enabled=false, the broker
        never receives a topology description for the group."""
        streams = StreamsTopologyDescriptionPluginService(
            self.test_context, self.kafka,
            extra_properties={"topology.description.push.enabled": "false"})
        streams.start()
        try:
            # Wait until the group exists and shows up under describe (NOT_STORED).
            def reports_not_stored():
                out, _ = _describe(self.kafka)
                return "No topology description has been recorded" in out
            wait_until(reports_not_stored,
                       timeout_sec=60,
                       err_msg="group never appeared in describe within 60s")

            # Sanity-check over a longer window: AVAILABLE should never be observed.
            import time
            deadline = time.time() + 60
            while time.time() < deadline:
                _, exit_code = _describe(self.kafka)
                assert exit_code != 0, "topology became AVAILABLE despite push.enabled=false"
                time.sleep(2)

            stdout, exit_code = _describe(self.kafka)
            assert exit_code != 0
            assert "No topology description has been recorded" in stdout, \
                "expected NOT_STORED message. stdout: %r" % stdout
        finally:
            streams.stop_nodes(clean_shutdown=True)


class StreamsTopologyDescriptionPluginNoPluginTest(Test):
    """Half of the rolling-upgrade matrix: with no plugin configured on the broker,
    the new RPC and the describe-topology surface degrade cleanly. The other half
    (pre-upgrade clients) is intrinsic to wire-version negotiation and not covered
    here."""

    def __init__(self, test_context):
        super().__init__(test_context=test_context)
        self.kafka = _build_kafka(test_context, plugin_enabled=False)

    def setUp(self):
        self.kafka.start()
        self.kafka.run_features_command("upgrade", "streams.version", 1)

    @cluster(num_nodes=2)
    @matrix(metadata_quorum=[quorum.combined_kraft])
    def test_no_plugin_broker_returns_not_stored(self, metadata_quorum):
        """A broker without the plugin configured cleanly reports NOT_STORED on
        describe and never asks the client to push."""
        streams = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        streams.start()
        try:
            def reports_not_stored():
                stdout, _ = _describe(self.kafka)
                return "No topology description has been recorded" in stdout
            wait_until(reports_not_stored,
                       timeout_sec=60,
                       err_msg="describe never reported NOT_STORED on plugin-less broker")
            stdout, exit_code = _describe(self.kafka)
            assert exit_code != 0, "expected non-zero exit, got %d. stdout: %s" % (exit_code, stdout)
        finally:
            streams.stop_nodes(clean_shutdown=True)
