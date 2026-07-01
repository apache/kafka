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
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for topology-description plugin interactions during group deletion
 * triggered by natural expiration. Two scenarios are covered:
 *
 * <ol>
 *   <li>A group with a stored topology description (storedDescriptionTopologyEpoch != -1)
 *       must have {@code plugin.deleteTopology} called by the periodic cleanup cycle before
 *       the group can be tombstoned by the offset-expiration sweep.</li>
 *   <li>A group where the topology push permanently failed (storedDescriptionTopologyEpoch == -1)
 *       is tombstoned directly by the offset-expiration sweep without any
 *       {@code plugin.deleteTopology} call.</li>
 * </ol>
 *
 * The cluster uses a short {@code offsets.retention.minutes} and a short
 * {@code offsets.retention.check.interval.ms} so the expiration sweep fires quickly enough
 * for an integration test.
 */
@Timeout(600)
@Tag("integration")
public class TopologyDescriptionPluginExpirationIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDescriptionPluginExpirationIntegrationTest.class);

    private static final int NUM_BROKERS = 1;

    /** 90 seconds: offsets.retention.minutes (60 s) plus ample buffer for sweep cycles. */
    private static final long EXPIRY_WAIT_MS = 90_000;

    private static final String CLUSTER_ID = "plugin-expiration";

    private static final Properties BROKER_CONFIG = new Properties();
    static {
        // Plugin must be set explicitly because addDefaultBrokerPropsIfAbsent uses putIfAbsent;
        // since we pre-populate the key here, the default is a no-op.
        BROKER_CONFIG.put(
            GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            InMemoryTopologyDescriptionPlugin.class.getName()
        );
        BROKER_CONFIG.put(InMemoryTopologyDescriptionPlugin.TEST_CLUSTER_ID_CONFIG, CLUSTER_ID);
        // Minimum allowed value; 1 minute = 60 000 ms.
        BROKER_CONFIG.put(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, "1");
        // Run the expiration sweep every 2 seconds so tests do not have to wait a full minute
        // between cycles after the offset has aged out.
        BROKER_CONFIG.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, "2000");
        // Allow short session timeouts so the group flips to EMPTY quickly after close().
        BROKER_CONFIG.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "1000");
    }

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    private static InMemoryTopologyDescriptionPlugin clusterPlugin;

    private final List<KafkaStreams> streamsInstances = new ArrayList<>();

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        clusterPlugin = InMemoryTopologyDescriptionPlugin.instanceForClusterId(CLUSTER_ID);
        assertNotNull(clusterPlugin, "No plugin instance found for cluster id " + CLUSTER_ID);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @AfterEach
    public void tearDown() {
        for (final KafkaStreams streams : streamsInstances) {
            streams.close(Duration.ofSeconds(30));
        }
        streamsInstances.clear();
    }

    private InMemoryTopologyDescriptionPlugin getPlugin() {
        assertNotNull(clusterPlugin, "Plugin should have been captured during cluster startup");
        return clusterPlugin;
    }

    private Properties streamsProperties(final String appId) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        return props;
    }

    private KafkaStreams startStreams(final org.apache.kafka.streams.Topology topology,
                                     final String appId) throws Exception {
        final KafkaStreams streams = new KafkaStreams(topology, streamsProperties(appId));
        streamsInstances.add(streams);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    private boolean groupAbsent(final Admin admin, final String appId) throws Exception {
        final Collection<GroupListing> groups = admin.listGroups(ListGroupsOptions.forStreamsGroups()).all().get();
        return groups.stream().noneMatch(g -> g.groupId().equals(appId));
    }

    /**
     * When a streams group with a stored topology description expires naturally (all members
     * gone, all offsets aged out), the periodic topology-cleanup cycle must call
     * {@code plugin.deleteTopology} and clear {@code storedDescriptionTopologyEpoch} before the
     * offset-expiration sweep tombstones the group.
     *
     * <p>Expected sequence: topology pushed → client closes → offsets age out
     * (≥ offsets.retention.minutes) → cleanup cycle calls {@code deleteTopology} → next sweep
     * tombstones the group.
     */
    @Test
    public void shouldCallDeleteTopologyBeforeExpiredGroupTombstoned(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;
        CLUSTER.createTopic(inputTopic);

        final InMemoryTopologyDescriptionPlugin plugin = getPlugin();

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);
        final KafkaStreams streams = startStreams(builder.build(), appId);

        // Wait for the topology description to reach the plugin.
        TestUtils.waitForCondition(
            () -> plugin.storedTopology(appId).isPresent(),
            60_000,
            "Topology for group " + appId + " was not pushed to the plugin"
        );
        LOG.info("Topology pushed to plugin for group {}", appId);

        streams.close(Duration.ofSeconds(30));
        LOG.info("Streams closed for group {}; waiting for expiry + cleanup cycle", appId);

        // The offset committed at close must age past offsets.retention.minutes (1 min = 60 s),
        // then the topology-cleanup cycle runs deleteTopology and the sweep tombstones the group.
        try (final Admin admin = CLUSTER.createAdminClient()) {
            TestUtils.waitForCondition(
                () -> groupAbsent(admin, appId),
                EXPIRY_WAIT_MS,
                "Group " + appId + " was not tombstoned within " + EXPIRY_WAIT_MS + " ms"
            );
        }

        assertTrue(plugin.storedTopology(appId).isEmpty(),
            "Plugin still holds a topology for group " + appId + " after expiry tombstone");
        assertTrue(plugin.getDeleteTopologyCount() > 0,
            "plugin.deleteTopology was never called for group " + appId);
    }

    /**
     * When a streams group whose topology push permanently failed (storedDescriptionTopologyEpoch
     * stays at -1) expires naturally, the offset-expiration sweep tombstones the group directly
     * without calling {@code plugin.deleteTopology}, because there is no stored topology epoch
     * to clean up.
     *
     * <p>Expected sequence: topology push permanently rejected → client closes → offsets age out
     * → sweep tombstones the group without any {@code deleteTopology} call.
     */
    @Test
    public void shouldTombstoneExpiredGroupWithoutCallingDeleteTopologyWhenNoTopologyStored(
        final TestInfo testInfo) throws Exception {

        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;
        CLUSTER.createTopic(inputTopic);

        final InMemoryTopologyDescriptionPlugin plugin = getPlugin();
        final int deleteCountBefore = plugin.getDeleteTopologyCount();
        // Topology push happens during initial join, before startApplicationAndWaitUntilRunning
        // returns; capture before startStreams so we don't miss the one and only push attempt.
        final int attemptsBefore = plugin.getSetTopologyAttemptCount();

        plugin.setFailOnSetPermanent(true);
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream(inputTopic);
            final KafkaStreams streams = startStreams(builder.build(), appId);

            // Wait for at least one (permanently-rejected) setTopology attempt so the broker has
            // ratcheted FailedDescriptionTopologyEpoch and will not re-solicit.
            TestUtils.waitForCondition(
                () -> plugin.getSetTopologyAttemptCount() > attemptsBefore,
                60_000,
                "Plugin never received a setTopology attempt for group " + appId
            );

            streams.close(Duration.ofSeconds(30));
            LOG.info("Streams closed for group {} (storedEpoch=-1); waiting for expiry tombstone", appId);
        } finally {
            plugin.setFailOnSetPermanent(false);
        }

        // With storedDescriptionTopologyEpoch == -1, the sweep tombstones directly once offsets age out.
        try (final Admin admin = CLUSTER.createAdminClient()) {
            TestUtils.waitForCondition(
                () -> groupAbsent(admin, appId),
                EXPIRY_WAIT_MS,
                "Group " + appId + " was not tombstoned within " + EXPIRY_WAIT_MS + " ms"
            );
        }

        assertEquals(deleteCountBefore, plugin.getDeleteTopologyCount(),
            "plugin.deleteTopology should not have been called for group " + appId
                + " because storedDescriptionTopologyEpoch was never set");
    }
}
