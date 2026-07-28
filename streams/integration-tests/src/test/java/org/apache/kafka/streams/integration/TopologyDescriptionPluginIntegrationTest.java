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
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests of the topology description push lifecycle (KIP-1331) on a live cluster:
 * a real {@link KafkaStreams} client pushes its topology description when the broker solicits
 * it on a heartbeat, and the broker stores it via the configured
 * {@link TrackingTopologyDescriptionPlugin}.
 */
@Timeout(600)
@Tag("integration")
public class TopologyDescriptionPluginIntegrationTest {

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties props = new Properties();
        props.put(
            GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            TrackingTopologyDescriptionPlugin.class.getName());
        // Frequent heartbeats keep the solicitation round-trips short.
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "200");
        // Effectively disable the periodic cleanup cycle and expiration sweep (both run at
        // this interval, 10 min by default): shouldInvokeDeleteTopologyOnDeleteGroups
        // relies on the explicit DeleteGroups call being the only delete path, and a slow
        // run crossing the default interval could otherwise expire the empty, offset-less
        // group first.
        props.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, String.valueOf(Integer.MAX_VALUE));
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
    public void shouldPushTopologyDescriptionToPluginAfterJoin() throws Exception {
        final String appId = "topology-description-push-app";
        final String inputTopic = "topology-description-push-input";
        cluster.createTopic(inputTopic, 1, 1);

        try (final Admin admin = createAdmin();
             final KafkaStreams streams = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
            startApplicationAndWaitUntilRunning(streams);

            waitForTopologyDescriptionAvailable(admin, appId);
            assertTrue(TrackingTopologyDescriptionPlugin.setTopologyCalls(appId) >= 1,
                "Expected at least one setTopology call for group " + appId);

            // The stored description carries the source topic.
            final StreamsGroupTopologyDescription topologyDescription =
                describeGroup(admin, appId, true).topologyDescription().orElseThrow();
            assertEquals(1, topologyDescription.subtopologies().size());
            final boolean sourceTopicFound = topologyDescription.subtopologies().stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .filter(StreamsGroupTopologyDescription.Source.class::isInstance)
                .map(StreamsGroupTopologyDescription.Source.class::cast)
                .anyMatch(source -> source.topics().contains(inputTopic));
            assertTrue(sourceTopicFound, "Expected a source node reading from " + inputTopic);

            // Describing without requesting the topology description must not attach one.
            final StreamsGroupDescription withoutTopology = describeGroup(admin, appId, false);
            assertEquals(StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED, withoutTopology.topologyDescriptionStatus());
            assertTrue(withoutTopology.topologyDescription().isEmpty());
        }
    }

    @Test
    public void shouldPushTopologyDescriptionOnlyOnceForTwoMembers() throws Exception {
        final String appId = "topology-description-dedup-app";
        final String inputTopic = "topology-description-dedup-input";
        cluster.createTopic(inputTopic, 2, 1);

        try (final Admin admin = createAdmin();
             final KafkaStreams streams1 = new KafkaStreams(topology(inputTopic), streamsConfig(appId));
             final KafkaStreams streams2 = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
            startApplicationAndWaitUntilRunning(List.of(streams1, streams2));

            waitForTopologyDescriptionAvailable(admin, appId);
            TestUtils.waitForCondition(
                () -> describeGroup(admin, appId, false).members().size() == 2,
                "Expected two members in group " + appId);

            // The broker solicits the push from a single member at a time and stops
            // soliciting once the description is stored, so a two-member group must
            // trigger exactly one setTopology call. Accepted flake risk: the client
            // retries the push on a retriable send failure (e.g. a disconnect after the
            // broker already processed the request), which would produce a duplicate
            // setTopology call at the same epoch — client-retry noise, not a dedup
            // regression. If this assertion ever flakes, that is why.
            assertEquals(1, TrackingTopologyDescriptionPlugin.setTopologyCalls(appId),
                "Expected exactly one setTopology call for group " + appId);
        }
    }

    @Test
    public void shouldStopSolicitingPushAfterPermanentPluginFailure() throws Exception {
        final String appId = "topology-description-failure-app";
        final String inputTopic = "topology-description-failure-input";
        cluster.createTopic(inputTopic, 1, 1);
        TrackingTopologyDescriptionPlugin.failPermanently(appId);

        try (final Admin admin = createAdmin();
             final KafkaStreams streams = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
            startApplicationAndWaitUntilRunning(streams);

            TestUtils.waitForCondition(
                () -> TrackingTopologyDescriptionPlugin.setTopologyCalls(appId) >= 1,
                "Expected a setTopology call for group " + appId);

            // The permanent failure ratchets the group's failed topology epoch, so the broker
            // must not re-solicit the push: the call count stays at one across many heartbeat
            // intervals, the application keeps running, and describe reports NOT_STORED.
            // Note: this window is shorter than the 30s initial re-solicitation back-off, so
            // it cannot distinguish the permanent-failure ratchet from an armed back-off (a
            // permanent failure misclassified as transient would also stay at one call here);
            // that classification is pinned down at the unit level by
            // GroupCoordinatorServiceTopologyDescriptionTest. What this does catch is the
            // ratchet being lost entirely, since re-solicitation would then happen within a
            // couple of heartbeats.
            Thread.sleep(2000);
            assertEquals(1, TrackingTopologyDescriptionPlugin.setTopologyCalls(appId),
                "Expected no re-solicitation after a permanent plugin failure for group " + appId);
            assertEquals(KafkaStreams.State.RUNNING, streams.state());
            assertEquals(StreamsGroupTopologyDescriptionStatus.NOT_STORED,
                describeGroup(admin, appId, true).topologyDescriptionStatus());
        }
    }

    @Test
    public void shouldInvokeDeleteTopologyOnDeleteGroups() throws Exception {
        final String appId = "topology-description-delete-app";
        final String inputTopic = "topology-description-delete-input";
        cluster.createTopic(inputTopic, 1, 1);

        try (final Admin admin = createAdmin()) {
            try (final KafkaStreams streams = new KafkaStreams(topology(inputTopic), streamsConfig(appId))) {
                startApplicationAndWaitUntilRunning(streams);
                waitForTopologyDescriptionAvailable(admin, appId);
            }

            // The application has left the group; once it is empty it can be deleted.
            TestUtils.waitForCondition(
                () -> describeGroup(admin, appId, false).members().isEmpty(),
                "Expected group " + appId + " to become empty");
            assertEquals(0, TrackingTopologyDescriptionPlugin.deleteTopologyCalls(appId));

            admin.deleteStreamsGroups(List.of(appId)).all().get();

            assertTrue(TrackingTopologyDescriptionPlugin.deleteTopologyCalls(appId) >= 1,
                "Expected DeleteGroups to invoke deleteTopology for group " + appId);
            final ExecutionException exception = assertThrows(ExecutionException.class,
                () -> admin.describeStreamsGroups(List.of(appId)).all().get());
            assertInstanceOf(GroupIdNotFoundException.class, exception.getCause());
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

    private static StreamsGroupDescription describeGroup(
        final Admin admin,
        final String groupId,
        final boolean includeTopologyDescription
    ) throws Exception {
        return admin.describeStreamsGroups(
                List.of(groupId),
                new DescribeStreamsGroupsOptions().includeTopologyDescription(includeTopologyDescription))
            .describedGroups()
            .get(groupId)
            .get();
    }

    private static void waitForTopologyDescriptionAvailable(final Admin admin, final String groupId) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> describeGroup(admin, groupId, true).topologyDescriptionStatus() == StreamsGroupTopologyDescriptionStatus.AVAILABLE,
            "Expected the topology description of group " + groupId + " to become AVAILABLE");
    }
}
