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
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import (
    INMEMORY_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS,
    StreamsUpgradeTestJobRunnerService,
)
from kafkatest.version import LATEST_4_2, LATEST_4_3, DEV_BRANCH, KafkaVersion


class StreamsProtocolCrossVersionTest(Test):
    """
    Cross-version tests for the streams rebalance protocol (KIP-1071).

    AK 4.4 bumped StreamsGroupHeartbeat (apiKey 88) and StreamsGroupDescribe (apiKey 89) from
    version 0 to version 1. The new fields are only exchanged when both the client and the broker
    speak v1, so mixed-version deployments must keep working:

      - StreamsGroupHeartbeatRequest v1 is byte-identical to v0. It was bumped only so that
        response v1 is negotiated, and so the broker may return the MISSING_CLIENT_TAGS status
        (code 6) that v0 clients do not know about (KAFKA-20744).
      - TaskOffsets/TaskEndOffsets exist in request v0, but pre-4.4 brokers reject any non-null
        value with InvalidRequestException ("TaskOffsets are not supported yet."). A newer client
        must strip the fields when v0 is negotiated (StreamsGroupHeartbeatRequest.Builder#build).
      - StreamsGroupHeartbeatResponse v1 replaces the v0 `AcceptableRecoveryLagLegacy` (int32)
        with `AcceptableRecoveryLag` (int64, ignorable) and adds `TopologyDescriptionRequired`
        (KIP-1331).
      - StreamsGroupDescribeRequest v1 adds `IncludeTopologyDescription`, and the response adds
        `TopologyDescription`, `TopologyDescriptionStatus` and `AssignorName` (KIP-1331, KIP-1357).

    4.2 is the earliest release that speaks the protocol at all -- `streams.version` level 1
    requires metadata version 4.2-IV1 -- so 4.2 and 4.3 are the only older versions worth pairing
    against a newer broker or client. The newer side is always the current branch (DEV); the
    same-version DEV/DEV combinations are covered by unit and integration tests instead.

    These tests drive the `StreamsUpgradeTest` harness, which is published in the 4.2/4.3 streams
    test jars and mirrored for trunk, so the same topology can be run on either side of the bump.
    """

    # Topics the StreamsUpgradeTest harness reads from and writes to.
    input_topic = "data"
    output_topic = "echo"

    # application.id hard-coded by the StreamsUpgradeTest harness, in every version.
    group_id = "StreamsUpgradeTest"

    RUNNING_LOG = "State transition from REBALANCING to RUNNING"

    # Logged by StreamsGroupHeartbeatRequestManager#describeConfig when the broker leaves
    # acceptableRecoveryLag at its protocol default, i.e. when the response came back as v0.
    # Only clients that speak response v1 (4.4+) log this line at all. The positive counterpart
    # -- a v1 response gets its actual value logged -- is unit-tested in
    # StreamsGroupHeartbeatRequestManagerTest#testLogsReceivedGroupConfigWhenAnyValueChanges.
    OLD_BROKER_LAG_LOG = "acceptableRecoveryLag=not provided (older broker)"

    # StatusDetail of the MISSING_CLIENT_TAGS status (code 6), which the group coordinator only
    # returns on a v1 heartbeat.
    MISSING_CLIENT_TAGS_LOG = "Missing required client tags for rack-aware standby assignment"

    UNSUPPORTED_VERSION_LOG = "UnsupportedVersionException"

    # On startup the client probes the broker for KIP-714 telemetry support; a broker without a
    # client-metrics receiver rejects GET_TELEMETRY_SUBSCRIPTIONS and the client logs the
    # rejection (with a stack trace) at DEBUG before disabling telemetry. That happens against
    # brokers of every version, including dev, and is unrelated to the streams protocol, so it
    # must not trip the UnsupportedVersionException guard.
    BENIGN_TELEMETRY_UVE_LOG = "does not support GET_TELEMETRY_SUBSCRIPTIONS"

    # A pre-4.4 group coordinator rejects a heartbeat that carries fields it does not support
    # with an InvalidRequestException naming the field ("TaskOffsets are not supported yet.",
    # "Static membership is not yet supported."); the client logs the message from the error
    # response. None of these strings may ever show up in a client log: the client must not have
    # sent such a field. The patterns name the fields specifically because every Streams client
    # also logs a benign, purely client-side StreamsConfig warning containing "not supported yet"
    # ("Warmup replicas are not supported yet with the streams protocol and will be ignored").
    OLD_BROKER_REJECTION_PATTERN = ("TaskOffsets are not supported|TaskEndOffsets are not supported"
                                    "|WarmupTasks are not supported|not yet supported"
                                    "|InvalidRequestException")

    RACK_AWARE_TAG = "zone"

    def __init__(self, test_context):
        super(StreamsProtocolCrossVersionTest, self).__init__(test_context=test_context)
        self.topics = {
            self.input_topic: {"partitions": 1, "replication-factor": 1},
            self.output_topic: {"partitions": 1, "replication-factor": 1},
        }

    def setup_kafka(self, broker_version, extra_server_prop_overrides=None):
        server_prop_overrides = [
            # The harness configures session.timeout.ms=10000, which is below the default lower bound.
            ["group.streams.min.session.timeout.ms", "10000"],
            ["group.streams.session.timeout.ms", "10000"],
        ]
        server_prop_overrides.extend(extra_server_prop_overrides or [])

        self.kafka = KafkaService(self.test_context,
                                 num_nodes=1,
                                 zk=None,
                                 topics=self.topics,
                                 server_prop_overrides=server_prop_overrides)
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

    def start_processor(self, client_version, extra_configs=None):
        """
        Start a single Streams instance on the streams rebalance protocol and wait until it is
        RUNNING. Reaching RUNNING is itself the core protocol assertion: it requires a successful
        join, topology initialization, an assignment from the group coordinator, and task creation.
        """
        processor = StreamsUpgradeTestJobRunnerService(self.test_context, self.kafka)
        # An empty version string makes kafka-run-class.sh use the trunk build rather than an
        # installed release, which is how the harness selects the DEV client.
        processor.set_version("" if client_version == str(DEV_BRANCH) else client_version)
        processor.set_config("group.protocol", "streams")
        for key, value in (extra_configs or {}).items():
            processor.set_config(key, value)

        with processor.node.account.monitor_log(processor.LOG_FILE) as monitor:
            processor.start()
            monitor.wait_until(self.RUNNING_LOG,
                               timeout_sec=120,
                               err_msg="Streams client (version '%s') never reached RUNNING on %s"
                                       % (client_version, str(processor.node.account)))
        return processor

    def count_in_file(self, node, literal, path):
        """Number of lines in `path` containing `literal`, matched as a fixed string."""
        cmd = "grep -c -F -- '%s' %s 2>/dev/null || true" % (literal, path)
        output = node.account.ssh_output(cmd, allow_fail=False).decode("utf-8", errors="replace").strip()
        return int(output) if output.isdigit() else 0

    def count_unsupported_version_errors(self, node, path):
        """
        Number of UnsupportedVersionException lines in `path`, ignoring the benign KIP-714
        telemetry probe (see BENIGN_TELEMETRY_UVE_LOG).
        """
        cmd = "grep -F -- '%s' %s 2>/dev/null | grep -c -v -F -- '%s' || true" \
              % (self.UNSUPPORTED_VERSION_LOG, path, self.BENIGN_TELEMETRY_UVE_LOG)
        output = node.account.ssh_output(cmd, allow_fail=False).decode("utf-8", errors="replace").strip()
        return int(output) if output.isdigit() else 0

    def count_old_broker_rejections(self, node, path):
        """Number of lines in `path` showing a broker-side heartbeat field rejection."""
        cmd = "grep -c -E -- '%s' %s 2>/dev/null || true" % (self.OLD_BROKER_REJECTION_PATTERN, path)
        output = node.account.ssh_output(cmd, allow_fail=False).decode("utf-8", errors="replace").strip()
        return int(output) if output.isdigit() else 0

    def assert_clean_cross_version_session(self, processor, other_side):
        """
        The catch-all compatibility assertions for a mixed-version client/broker pairing: the
        client must not have hit a version negotiation failure or a broker-side field rejection,
        and the broker must not have logged an ERROR while serving the session. A rejected
        heartbeat surfaces on both sides -- the coordinator answers INVALID_REQUEST with the
        rejection message, which the client logs -- so both logs are checked.
        """
        assert self.count_unsupported_version_errors(processor.node, processor.LOG_FILE) == 0, \
            "The client hit an UnsupportedVersionException against a %s counterpart" % other_side
        assert self.count_old_broker_rejections(processor.node, processor.LOG_FILE) == 0, \
            "The %s broker rejected a heartbeat field the client should not have sent " \
            "(e.g. TaskOffsets/TaskEndOffsets, which must be stripped at request v0)" % other_side
        server_log = KafkaService.OPERATIONAL_LOG_INFO_DIR + "/server.log"
        broker_errors = self.count_in_file(self.kafka.nodes[0], " ERROR ", server_log)
        assert broker_errors == 0, \
            "The %s broker logged %d ERROR line(s) while serving a cross-version session" \
            % (other_side, broker_errors)

    def run_streams_groups_command(self, args, tool_version=None):
        """
        Run bin/kafka-streams-groups.sh against the cluster, from the install of `tool_version`
        (the trunk build when omitted). Returns the combined stdout/stderr; the command is allowed
        to fail so callers can assert on how a failure is reported.
        """
        node = self.kafka.nodes[0]
        version = DEV_BRANCH if tool_version is None else KafkaVersion(tool_version)
        script = self.kafka.path.script("kafka-streams-groups.sh", version)
        cmd = "%s --bootstrap-server %s %s 2>&1" % (script, self.kafka.bootstrap_servers(), args)
        self.logger.info("Running streams-groups command: %s" % cmd)
        return node.account.ssh_output(cmd, allow_fail=True).decode("utf-8", errors="replace")

    @cluster(num_nodes=2)
    @matrix(broker_version=[str(LATEST_4_2), str(LATEST_4_3)],
            metadata_quorum=[quorum.combined_kraft])
    def test_new_client_old_broker(self, broker_version, metadata_quorum):
        """
        A client from the current branch (DEV) must work against a 4.2/4.3 broker: it must not
        send a request the older broker rejects, and it must tolerate a v0 response in which the
        new int64 AcceptableRecoveryLag is absent and therefore reads back as its default of -1.

        The sharpest edge is task offsets: the DEV client populates TaskOffsets/TaskEndOffsets
        (on join, and again whenever its assignment or offsets change), the fields exist in
        request v0, and a 4.2/4.3 broker rejects any non-null value with InvalidRequestException
        ("TaskOffsets are not supported yet."). The client must strip them when v0 is negotiated
        (unit-tested in StreamsGroupHeartbeatRequestTest#testBuildClearsTaskOffsetsForVersion0);
        a clean session here proves the strip end to end against the real released broker.
        """
        self.setup_kafka(broker_version)
        processor = self.start_processor(str(DEV_BRANCH))

        lag_not_provided = self.count_in_file(processor.node, self.OLD_BROKER_LAG_LOG, processor.LOG_FILE)
        assert lag_not_provided > 0, \
            "Against a %s broker the heartbeat response is v0 and carries no int64 " \
            "acceptableRecoveryLag, so the client should have logged it as not provided" \
            % broker_version

        self.assert_clean_cross_version_session(processor, broker_version)

        processor.stop()

    @cluster(num_nodes=2)
    @matrix(client_version=[str(LATEST_4_2), str(LATEST_4_3)],
            metadata_quorum=[quorum.combined_kraft])
    def test_old_client_new_broker(self, client_version, metadata_quorum):
        """
        A 4.2/4.3 client must keep working against a DEV broker. The broker sets the v1-only
        AcceptableRecoveryLag unconditionally and relies on it being `ignorable` so that it is
        dropped when the response is serialized at v0; an older client must not see a malformed
        response, and the broker must not log an error serving the v0 session.
        """
        self.setup_kafka(str(DEV_BRANCH))
        processor = self.start_processor(client_version)

        self.assert_clean_cross_version_session(processor, str(DEV_BRANCH))

        processor.stop()

    @cluster(num_nodes=2)
    @matrix(client_version=[str(LATEST_4_2), str(LATEST_4_3)],
            metadata_quorum=[quorum.combined_kraft])
    def test_missing_client_tags_status_not_sent_to_old_client(self, client_version, metadata_quorum):
        """
        With rack-aware assignment tags required on the broker but absent on the client, the
        group coordinator returns the MISSING_CLIENT_TAGS status (code 6). That status is gated
        on heartbeat version >= 1 because the Status enum shipped in 4.2/4.3 defines codes 0-5
        only, so a 4.2/4.3 client must never receive it. The positive halves of the gate -- a v1
        client does receive the status, and a v1 client with the tag configured does not -- are
        unit-tested in GroupMetadataManagerTest#testMissingClientTagsStatus* and
        StreamsGroupHeartbeatRequestManagerTest, so this pins only the cross-version negative
        against real released clients.
        """
        self.setup_kafka(str(DEV_BRANCH),
                         extra_server_prop_overrides=[
                             ["group.streams.rack.aware.assignment.tags", self.RACK_AWARE_TAG]
                         ])
        # No client.tag.<tag> is configured, so the required tag is missing.
        processor = self.start_processor(client_version)

        status_logged = self.count_in_file(processor.node, self.MISSING_CLIENT_TAGS_LOG, processor.LOG_FILE)
        assert status_logged == 0, \
            "The broker returned the MISSING_CLIENT_TAGS status to a %s client, which " \
            "negotiates heartbeat v0 and does not understand status code 6" % client_version

        processor.stop()

    @cluster(num_nodes=2)
    @matrix(broker_version=[str(LATEST_4_2), str(LATEST_4_3)],
            metadata_quorum=[quorum.combined_kraft])
    def test_describe_new_tool_old_broker(self, broker_version, metadata_quorum):
        """
        The DEV kafka-streams-groups.sh must describe a group hosted on a 4.2/4.3 broker. Plain
        --describe and --describe --state stay within describe v0 and must succeed; the ASSIGNOR
        column added for KIP-1357 must render empty, because describe v0 has no AssignorName
        field for the broker to fill.

        --describe --topology cannot be served by a v0 broker: IncludeTopologyDescription is a
        v1-only, non-ignorable field, so serializing it at v0 raises UnsupportedVersionException
        (see RequestResponseTest#testStreamsGroupDescribeRequestV0RejectsIncludeTopologyDescription).
        The command must still fail in a diagnosable way rather than hang. Degrading gracefully --
        the way --delete-offsets already reports an unsupported broker version -- is an open
        follow-up, so this asserts only that the failure names the unsupported field or version.
        """
        self.setup_kafka(broker_version)
        processor = self.start_processor(str(DEV_BRANCH))

        output = self.run_streams_groups_command("--describe --group %s" % self.group_id)
        assert self.group_id in output, \
            "The DEV tool did not describe the group on a %s broker. Output:\n%s" % (broker_version, output)
        assert "OFFSET-LAG" in output, \
            "The DEV tool did not print the offsets table for a %s broker. Output:\n%s" % (broker_version, output)
        assert "Error:" not in output, \
            "Plain --describe reported an error against a %s broker. Output:\n%s" % (broker_version, output)

        state_output = self.run_streams_groups_command("--describe --state --group %s" % self.group_id)
        assert self.group_id in state_output and "ASSIGNOR" in state_output, \
            "The DEV tool did not print the state table for a %s broker. Output:\n%s" \
            % (broker_version, state_output)
        assert "Error:" not in state_output, \
            "--describe --state reported an error against a %s broker. Output:\n%s" \
            % (broker_version, state_output)
        assert "sticky" not in state_output, \
            "--describe --state against a %s broker showed an assignor name, but describe v0 " \
            "has no AssignorName field, so the column must be empty. Output:\n%s" \
            % (broker_version, state_output)

        topology_output = self.run_streams_groups_command("--describe --topology --group %s" % self.group_id)
        assert "Error:" in topology_output, \
            "--describe --topology unexpectedly succeeded against a %s broker, which cannot " \
            "serve describe v1. Output:\n%s" % (broker_version, topology_output)
        assert ("includeTopologyDescription" in topology_output
                or "UnsupportedVersion" in topology_output
                or "not supported by the broker" in topology_output), \
            "--describe --topology failed against a %s broker without identifying the version " \
            "incompatibility. Output:\n%s" % (broker_version, topology_output)

        processor.stop()

    @cluster(num_nodes=2)
    @matrix(tool_version=[str(LATEST_4_2), str(LATEST_4_3)],
            metadata_quorum=[quorum.combined_kraft])
    def test_describe_old_tool_new_broker(self, tool_version, metadata_quorum):
        """
        The other tooling direction: a 4.2/4.3 kafka-streams-groups.sh describes a group on a DEV
        broker. The older tool sends describe v0 and has no --topology flag, so the broker must
        answer at v0 and leave out TopologyDescription, TopologyDescriptionStatus and AssignorName.
        """
        self.setup_kafka(str(DEV_BRANCH))
        processor = self.start_processor(str(DEV_BRANCH))

        output = self.run_streams_groups_command("--describe --group %s" % self.group_id,
                                                 tool_version=tool_version)
        assert self.group_id in output, \
            "The %s tool did not describe the group on a DEV broker. Output:\n%s" % (tool_version, output)
        assert "OFFSET-LAG" in output, \
            "The %s tool did not print the offsets table against a DEV broker. Output:\n%s" % (tool_version, output)
        assert "Error:" not in output, \
            "The %s tool reported an error describing a group on a DEV broker. Output:\n%s" % (tool_version, output)

        processor.stop()

    @cluster(num_nodes=2)
    @matrix(client_version=[str(LATEST_4_2), str(LATEST_4_3)],
            metadata_quorum=[quorum.combined_kraft])
    def test_topology_description_not_stored_for_old_client(self, client_version, metadata_quorum):
        """
        A DEV broker with the topology description plugin configured solicits a description push
        through TopologyDescriptionRequired, which only exists in heartbeat response v1. A 4.2/4.3
        client never receives the flag and never pushes, so the broker must report that no
        description is stored rather than serving a stale or empty one.
        """
        self.setup_kafka(str(DEV_BRANCH),
                         extra_server_prop_overrides=[
                             ["group.streams.topology.description.plugin.class",
                              INMEMORY_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS]
                         ])
        processor = self.start_processor(client_version)

        output = self.run_streams_groups_command("--describe --topology --group %s" % self.group_id)
        assert "No topology description is stored" in output, \
            "Expected the broker to report no stored topology description for a %s client, which " \
            "cannot be asked to push one. Output:\n%s" % (client_version, output)

        processor.stop()
