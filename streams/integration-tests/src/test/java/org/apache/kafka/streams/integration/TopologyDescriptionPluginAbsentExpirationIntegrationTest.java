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
import org.apache.kafka.common.errors.GroupIdNotFoundException;
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
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that a streams group expires normally on a broker with no topology description plugin
 * configured. {@code StreamsGroup.shouldExpire} only defers the group tombstone while
 * a plugin is configured and a stored topology epoch is still pending cleanup; with no plugin
 * there is nothing to clean up, so the offset-expiration sweep must tombstone the idle group
 * directly instead of waiting for a cleanup cycle that never runs.
 *
 * <p>{@link EmbeddedKafkaCluster} configures an in-memory plugin by default for streams
 * integration tests, so this test opts out explicitly. The test avoids producing any input data,
 * so the group never commits offsets and becomes eligible for expiration as soon as its last
 * member leaves.
 */
@Timeout(600)
@Tag("integration")
public class TopologyDescriptionPluginAbsentExpirationIntegrationTest {

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties props = new Properties();
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            EmbeddedKafkaCluster.NO_TOPOLOGY_DESCRIPTION_PLUGIN);
        // Run the offset-expiration sweep frequently so expiry happens within seconds.
        props.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, "1000");
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "200");
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
    public void shouldTombstoneExpiredGroupWhenNoPluginIsConfigured() throws Exception {
        final String appId = "topology-description-absent-expiration-app";
        final String inputTopic = "topology-description-absent-expiration-input";
        cluster.createTopic(inputTopic, 1, 1);

        try (final Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            try (final KafkaStreams streams = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
                startApplicationAndWaitUntilRunning(streams);
                // Without a plugin the broker never solicits a push, so no stored epoch exists
                // that could defer the tombstone.
                assertEquals(StreamsGroupTopologyDescriptionStatus.NOT_STORED,
                    describeGroup(admin, appId).topologyDescriptionStatus());
            }

            // The member has left and the group has no committed offsets; with no plugin
            // configured the expiration sweep must tombstone it without any cleanup step.
            waitForGroupToBeDeleted(admin, appId);
        }
    }

    private static Topology topology(final String inputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .foreach((key, value) -> { });
        return builder.build();
    }

    private static Properties streamsConfig(final String appId) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        return config;
    }

    private static StreamsGroupDescription describeGroup(final Admin admin, final String groupId) throws Exception {
        return admin.describeStreamsGroups(
                List.of(groupId),
                new DescribeStreamsGroupsOptions().includeTopologyDescription(true))
            .describedGroups()
            .get(groupId)
            .get();
    }

    private static void waitForGroupToBeDeleted(final Admin admin, final String groupId) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                describeGroup(admin, groupId);
                return false;
            } catch (final ExecutionException exception) {
                if (exception.getCause() instanceof GroupIdNotFoundException) {
                    return true;
                }
                // Unexpected failures (e.g. admin timeouts) must not be folded into
                // "group not yet deleted": waitForCondition retries a throwing condition
                // and rethrows the last exception at timeout, preserving the diagnostics.
                throw exception;
            }
        }, "Expected group " + groupId + " to be tombstoned by the expiration sweep");
    }
}
