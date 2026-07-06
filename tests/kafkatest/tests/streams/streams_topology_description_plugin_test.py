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

import time

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.streams import (
    INMEMORY_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS,
    StreamsTopologyDescriptionPluginService,
)


class StreamsTopologyDescriptionPluginTest(Test):

    PUSH_REQUESTED_LOG = "Broker requested topology description push"
    PUSH_SENDING_LOG = "Sending topology description for group"
    PUSH_SUCCESS_LOG = "Topology description pushed successfully"

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
        Test the situation when the broker has the topology description plugin configured
        but the client opts out via topology.description.push.enabled=false. The client
        should never send a topology description even though the broker solicits one.
        """
        self.setup_kafka(plugin_enabled=True)
        processor = StreamsTopologyDescriptionPluginService(
            self.test_context, self.kafka, topology_description_push_enabled=False)
        processor.start()
        time.sleep(30)
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
        The broker should never solicit a topology push from the client.
        """
        self.setup_kafka(plugin_enabled=False)
        processor = StreamsTopologyDescriptionPluginService(self.test_context, self.kafka)
        processor.start()
        time.sleep(30)
        # count how many lines in streams.log contain Broker requested topology description push
        solicited = processor.node.account.ssh_capture(
            "grep -c '%s' %s || true" % (self.PUSH_REQUESTED_LOG, processor.LOG_FILE),
            allow_fail=False)
        assert int(next(solicited).strip()) == 0, \
            "Broker solicited a topology push even though no plugin was configured"
        processor.stop()
