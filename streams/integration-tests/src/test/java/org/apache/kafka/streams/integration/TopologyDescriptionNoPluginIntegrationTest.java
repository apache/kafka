/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescriptionStatus;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of a real {@link KafkaStreams} client against a broker running the
 * plugin-less production default (no topology description plugin configured, KIP-1331).
 * {@link EmbeddedKafkaCluster} configures an in-memory plugin by default for all streams
 * integration tests, so this test opts out explicitly to keep the production default
 * exercised at the streams level.
 */
@Timeout(600)
@Tag("integration")
public class TopologyDescriptionNoPluginIntegrationTest {

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties props = new Properties();
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            EmbeddedKafkaCluster.NO_TOPOLOGY_DESCRIPTION_PLUGIN);
        cluster = new EmbeddedKafkaCluster(1, props);
        cluster.start();
        bootstrapServers = cluster.bootstrapServers();
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
        cluster = null;
    }

    @Test
    public void shouldRunStreamsApplicationAgainstBrokerWithoutTopologyDescriptionPlugin() throws Exception {
        final String appId = "topology-description-no-plugin-app";
        final String inputTopic = "topology-description-no-plugin-input";
        cluster.createTopic(inputTopic, 1, 1);

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .foreach((key, value) -> { });
        final Topology topology = builder.build();

        try (final Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
             final KafkaStreams streams = new KafkaStreams(topology, config)) {
            // Without a plugin the broker never solicits a topology description push; the
            // application must join and run normally regardless.
            startApplicationAndWaitUntilRunning(streams);

            final StreamsGroupDescription description = admin.describeStreamsGroups(
                    List.of(appId),
                    new DescribeStreamsGroupsOptions().includeTopologyDescription(true))
                .describedGroups()
                .get(appId)
                .get();
            assertFalse(description.members().isEmpty());
            assertEquals(StreamsGroupTopologyDescriptionStatus.NOT_STORED, description.topologyDescriptionStatus());
            assertTrue(description.topologyDescription().isEmpty());
        }
    }
}
