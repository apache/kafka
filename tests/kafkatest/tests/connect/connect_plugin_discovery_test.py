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

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.directory_layout.kafka_path import CONNECT_FILE_JAR
from kafkatest.version import LATEST_3_5
from kafkatest.services.connect import ConnectStandaloneService, ConnectServiceBase
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until


class ConnectPluginDiscoveryTest(KafkaTest):
    """
    Test for the `plugin.discovery` configuration and accompanying `connect-plugin-path` migration script.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    OFFSETS_FILE = "/mnt/connect.offsets"
    PLUGIN_PATH = "/mnt/connect-plugin-path"

    def __init__(self, test_context):
        super(ConnectPluginDiscoveryTest, self).__init__(test_context, num_zk=1, num_brokers=1)

        self.cc = ConnectStandaloneService(test_context, self.kafka, [self.PLUGIN_PATH, self.OFFSETS_FILE])

    @cluster(num_nodes=3)
    @matrix(plugin_discovery=['only_scan', 'hybrid_warn', 'hybrid_fail', 'service_load'],
            command=[('sync-manifests', '--dry-run'), ('sync-manifests',)])
    def test_plugin_discovery_migration(self, plugin_discovery, command):
        # Template parameters
        self.PLUGIN_DISCOVERY = plugin_discovery

        self.cc.set_configs(lambda node: self.render("connect-standalone.properties", node=node))
        # Explicitly clean the plugin path now, because we won't clean it on start
        self.cc.clean()

        plugin_location = os.path.join(self.PLUGIN_PATH, "connect-file")
        for node in self.cc.nodes:
            # Copy a non-migrated plugin jar into the plugin path
            node.account.ssh("mkdir -p %s" % plugin_location)
            node.account.ssh("cp %s %s" % ((self.cc.path.jar(CONNECT_FILE_JAR, LATEST_3_5)), plugin_location))
            # Execute a connect-plugin-path command on the non-migrated plugin path
            node.account.ssh("%s %s %s %s" % (
                self.cc.path.script("connect-plugin-path.sh"), " ".join(command),
                '--plugin-path', self.PLUGIN_PATH))

        migrated = command == ('sync-manifests',)
        should_start = not (plugin_discovery == 'hybrid_fail' and not migrated)
        # Do not clean the plugin path during startup
        self.cc.start(mode=None if should_start else ConnectServiceBase.STARTUP_MODE_INSTANT, clean=False)

        if should_start:
            # plugins should be visible in the backwards-compatible modes, or if the migration finished.
            expect_discovered = plugin_discovery == 'only_scan' or plugin_discovery == 'hybrid_warn' or migrated
            actual_discovered = set([connector_plugin['class'] for connector_plugin in self.cc.list_connector_plugins()]).issuperset({self.FILE_SOURCE_CONNECTOR, self.FILE_SINK_CONNECTOR})
            assert expect_discovered == actual_discovered
        else:
            # The worker should crash due to non-migrated plugins
            for node in self.cc.nodes:
                wait_until(lambda: not node.account.alive(self.cc.pids(node)), timeout_sec=self.cc.startup_timeout_sec,
                           err_msg="Kafka Connect did not stop: %s" % str(node.account))
