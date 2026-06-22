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

/**
 * Verifies that a streams group expires and is tombstoned normally when no topology-description
 * plugin is configured on the broker. The {@code deferStreamsGroupTombstoneForPluginCleanup} gate
 * returns {@code false} when {@code topologyPluginConfigured=false}, so the offset-expiration
 * sweep must tombstone the group directly without any plugin interaction.
 *
 * <p>A separate cluster is used because {@link EmbeddedKafkaCluster#addDefaultBrokerPropsIfAbsent}
 * now enables the in-memory plugin by default; to suppress it, the plugin class key is
 * pre-populated with an empty string before the cluster starts.
 */
@Timeout(300)
@Tag("integration")
public class TopologyDescriptionPluginAbsentExpirationIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(
        TopologyDescriptionPluginAbsentExpirationIntegrationTest.class);

    private static final int NUM_BROKERS = 1;

    /** 90 seconds: offsets.retention.minutes (60 s) plus ample buffer for sweep cycles. */
    private static final long EXPIRY_WAIT_MS = 90_000;

    private static final Properties BROKER_CONFIG = new Properties();
    static {
        // Explicitly set to empty string so addDefaultBrokerPropsIfAbsent's putIfAbsent
        // does not install the in-memory plugin — this cluster runs without any plugin.
        BROKER_CONFIG.put(
            GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            ""
        );
        BROKER_CONFIG.put(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, "1");
        BROKER_CONFIG.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, "2000");
        BROKER_CONFIG.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "1000");
    }

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    private final List<KafkaStreams> streamsInstances = new ArrayList<>();

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
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

    private boolean groupAbsent(final Admin admin, final String appId) throws Exception {
        final Collection<GroupListing> groups = admin.listGroups(ListGroupsOptions.forStreamsGroups()).all().get();
        return groups.stream().noneMatch(g -> g.groupId().equals(appId));
    }

    /**
     * With no plugin configured, {@code deferStreamsGroupTombstoneForPluginCleanup} returns
     * {@code false} (short-circuits on {@code topologyPluginConfigured=false}), so the
     * offset-expiration sweep tombstones the group directly once offsets age out.
     */
    @Test
    public void shouldTombstoneExpiredGroupWhenNoPluginConfigured(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;
        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties(appId));
        streamsInstances.add(streams);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

        streams.close(Duration.ofSeconds(30));
        LOG.info("Streams closed for group {} (no plugin); waiting for expiry tombstone", appId);

        try (final Admin admin = CLUSTER.createAdminClient()) {
            TestUtils.waitForCondition(
                () -> groupAbsent(admin, appId),
                EXPIRY_WAIT_MS,
                "Group " + appId + " was not tombstoned within " + EXPIRY_WAIT_MS + " ms"
            );
        }
    }
}
