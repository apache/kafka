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
import org.apache.kafka.streams.integration.utils.TrackingTopologyDescriptionPlugin;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

/**
 * Tests of the topology description plugin behavior when a streams group expires naturally
 * (KIP-1331): the periodic broker-side cleanup cycle calls {@code plugin.deleteTopology} and
 * clears the stored topology epoch for empty groups without offsets, after which the regular
 * expiration sweep tombstones the group. Groups whose push permanently failed are left at
 * UNCERTAIN(-2) — the non-atomic plugin call and epoch write mean the broker cannot rule out a
 * residual plugin entry — so the cleanup cycle still issues {@code plugin.deleteTopology}
 * defensively before the tombstone.
 *
 * <p>The tests avoid producing any input data, so the groups never commit offsets and become
 * eligible for expiration as soon as their last member leaves.
 */
@Timeout(600)
@Tag("integration")
public class TopologyDescriptionPluginExpirationIntegrationTest {

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties props = new Properties();
        props.put(
            GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            TrackingTopologyDescriptionPlugin.class.getName());
        // Run the offset-expiration sweep and the topology-description cleanup cycle (both
        // are scheduled at this interval) frequently so expiry happens within seconds.
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

    @BeforeEach
    public void resetPlugin() {
        TrackingTopologyDescriptionPlugin.reset();
    }

    @Test
    public void shouldDeleteStoredTopologyDescriptionWhenGroupExpires() throws Exception {
        final String appId = "topology-description-expiration-app";
        final String inputTopic = "topology-description-expiration-input";
        cluster.createTopic(inputTopic, 1, 1);

        try (final Admin admin = createAdmin()) {
            try (final KafkaStreams streams = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
                startApplicationAndWaitUntilRunning(streams);
                TestUtils.waitForCondition(
                    () -> describeGroup(admin, appId).topologyDescriptionStatus() == StreamsGroupTopologyDescriptionStatus.AVAILABLE,
                    "Expected the topology description of group " + appId + " to become AVAILABLE");
            }

            // The member has left and the group has no committed offsets, so the cleanup
            // cycle must call plugin.deleteTopology and clear the stored epoch, after which
            // the expiration sweep tombstones the group.
            TestUtils.waitForCondition(
                () -> TrackingTopologyDescriptionPlugin.deleteTopologyCalls(appId) >= 1,
                "Expected the cleanup cycle to invoke deleteTopology for group " + appId);
            waitForGroupToBeDeleted(admin, appId);
        }
    }

    @Test
    public void shouldReclaimUncertainPluginStateWhenPushPermanentlyFailed() throws Exception {
        final String appId = "topology-description-failed-expiration-app";
        final String inputTopic = "topology-description-failed-expiration-input";
        cluster.createTopic(inputTopic, 1, 1);
        TrackingTopologyDescriptionPlugin.failPermanently(appId);

        try (final Admin admin = createAdmin()) {
            try (final KafkaStreams streams = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
                startApplicationAndWaitUntilRunning(streams);
                TestUtils.waitForCondition(
                    () -> TrackingTopologyDescriptionPlugin.setTopologyCalls(appId) >= 1,
                    "Expected a setTopology call for group " + appId);
            }

            // The permanent push failure leaves the group at UNCERTAIN(-2): the barrier written
            // before the plugin call remains, because the failed-epoch write only advances the
            // failed epoch. UNCERTAIN is delete-eligible, so the cleanup cycle must call
            // plugin.deleteTopology defensively to reclaim any residual entry before the
            // expiration sweep tombstones the group.
            TestUtils.waitForCondition(
                () -> TrackingTopologyDescriptionPlugin.deleteTopologyCalls(appId) >= 1,
                "Expected the cleanup cycle to invoke deleteTopology for group " + appId
                    + " left at UNCERTAIN by the permanent push failure");
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

    private static Admin createAdmin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
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
