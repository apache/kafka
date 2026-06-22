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
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.GlobalStore;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Node;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Processor;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Sink;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Source;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.Subtopology;
import org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
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
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that verifies the end-to-end topology description plugin flow:
 * a Kafka Streams client using the streams group protocol pushes its topology
 * description to the broker, where the configured plugin stores it.
 */
@Timeout(600)
@Tag("integration")
public class TopologyDescriptionPluginIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDescriptionPluginIntegrationTest.class);

    private static final int NUM_BROKERS = 1;

    private static final String CLUSTER_ID = "plugin-integration";

    private static final Properties BROKER_CONFIG = new Properties();
    static {
        BROKER_CONFIG.put(
            GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG,
            InMemoryTopologyDescriptionPlugin.class.getName()
        );
        BROKER_CONFIG.put(InMemoryTopologyDescriptionPlugin.TEST_CLUSTER_ID_CONFIG, CLUSTER_ID);
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
            streams.close(Duration.ofSeconds(60));
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

    private KafkaStreams startStreams(final Topology topology, final String appId) throws Exception {
        final KafkaStreams streams = new KafkaStreams(topology, streamsProperties(appId));
        streamsInstances.add(streams);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    private InMemoryTopologyDescriptionPlugin getPlugin() {
        assertNotNull(clusterPlugin, "Plugin should have been captured during cluster startup");
        return clusterPlugin;
    }

    private StreamsGroupTopologyDescription waitForTopology(final InMemoryTopologyDescriptionPlugin plugin,
                                                final String appId) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> plugin.storedTopology(appId).isPresent(),
            60_000,
            "Topology description was not pushed to the plugin within timeout"
        );
        return plugin.storedTopology(appId).get();
    }

    /**
     * Builds a topology that exercises all node types:
     * <ul>
     *   <li>Source node reading from inputTopic</li>
     *   <li>Processor node with a state store (via groupByKey().count())</li>
     *   <li>Sink node writing to outputTopic</li>
     *   <li>Global store backed by globalTopic</li>
     * </ul>
     */
    private Topology buildRichTopology(final String inputTopic, final String outputTopic,
                                       final String globalTopic, final String storeName) {
        final StreamsBuilder builder = new StreamsBuilder();

        // Source -> stateful processor (groupByKey + count with named store) -> sink
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(storeName))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        // Global table
        builder.globalTable(globalTopic, Consumed.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    @Test
    public void shouldPushFullTopologyDescriptionToPlugin(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;
        final String outputTopic = "output-" + testId;
        final String globalTopic = "global-" + testId;
        final String storeName = "my-count-store";

        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic, outputTopic, globalTopic);

        final Topology topology = buildRichTopology(inputTopic, outputTopic, globalTopic, storeName);
        LOG.info("Client-side topology:\n{}", topology.describe());

        startStreams(topology, appId);

        final InMemoryTopologyDescriptionPlugin plugin = getPlugin();
        final StreamsGroupTopologyDescription pluginTopology = waitForTopology(plugin, appId);
        LOG.info("Plugin received topology for group {}: {}", appId, pluginTopology);

        assertNotEquals(-1, plugin.storedDescriptionTopologyEpoch(appId));

        // -- Verify subtopologies --
        // groupByKey().count() introduces a repartition, producing two subtopologies:
        //   subtopology 0: source(inputTopic) -> processor -> sink(repartition topic)
        //   subtopology 1: source(repartition topic) -> aggregate processor (with store) -> sink(outputTopic)
        assertFalse(pluginTopology.subtopologies().isEmpty(), "Expected at least one subtopology");

        for (final Subtopology st : pluginTopology.subtopologies()) {
            LOG.info("Subtopology {}: {} nodes", st.id(), st.nodes().size());
            for (final Node node : st.nodes()) {
                LOG.info("  Node '{}': type={}, successors={}",
                    node.name(), node.getClass().getSimpleName(), node.successors());
            }
        }

        // Find the subtopology that reads from the input topic
        final Subtopology inputSubtopology = pluginTopology.subtopologies().stream()
            .filter(st -> st.nodes().stream().anyMatch(
                n -> n instanceof Source && ((Source) n).topics().contains(inputTopic)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No subtopology reads from " + inputTopic));

        // It should have a source node and a sink node (to the repartition topic)
        assertTrue(inputSubtopology.nodes().stream().anyMatch(n -> n instanceof Source),
            "Input subtopology should have a source node");
        assertTrue(inputSubtopology.nodes().stream().anyMatch(n -> n instanceof Sink),
            "Input subtopology should have a sink node (repartition)");

        // Find the subtopology that writes to the output topic
        final Subtopology outputSubtopology = pluginTopology.subtopologies().stream()
            .filter(st -> st.nodes().stream().anyMatch(
                n -> n instanceof Sink && ((Sink) n).topic().filter(outputTopic::equals).isPresent()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No subtopology writes to " + outputTopic));

        // It should contain a processor with the state store
        final List<Processor> processorNodes = outputSubtopology.nodes().stream()
            .filter(n -> n instanceof Processor)
            .map(n -> (Processor) n)
            .collect(Collectors.toList());
        assertFalse(processorNodes.isEmpty(), "Output subtopology should have processor nodes");

        final Set<String> allStores = processorNodes.stream()
            .flatMap(n -> n.stores().stream())
            .collect(Collectors.toSet());
        assertTrue(allStores.contains(storeName),
            "Expected store '" + storeName + "' in processor nodes but found " + allStores);

        // Verify successors are populated as expected (predecessors are reconstructed downstream).
        for (final Subtopology st : pluginTopology.subtopologies()) {
            for (final Node node : st.nodes()) {
                if (node instanceof Source) {
                    assertFalse(node.successors().isEmpty(),
                        "Source node '" + node.name() + "' should have successors");
                } else if (node instanceof Sink) {
                    assertTrue(node.successors().isEmpty(),
                        "Sink node '" + node.name() + "' should have no successors");
                }
            }
        }

        // -- Verify global store --
        assertFalse(pluginTopology.globalStores().isEmpty(), "Expected at least one global store");
        final GlobalStore globalStore = pluginTopology.globalStores().iterator().next();
        LOG.info("Global store: source='{}' (topics={}), processor='{}'",
            globalStore.source().name(), globalStore.source().topics(),
            globalStore.processor().name());

        assertTrue(globalStore.source().topics().contains(globalTopic),
            "Global store source should read from " + globalTopic);
        assertFalse(globalStore.processor().stores().isEmpty(),
            "Global store processor should have a state store");

        LOG.info("Full topology description verified: {} subtopologies, {} global stores",
            pluginTopology.subtopologies().size(), pluginTopology.globalStores().size());
    }

    @Test
    public void shouldRatchetOnPermanentFailureAndStopReSoliciting(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;
        CLUSTER.createTopic(inputTopic, 1, 1);

        final InMemoryTopologyDescriptionPlugin plugin = getPlugin();
        plugin.setFailOnSetPermanent(true);
        try {
            final int attemptsBefore = plugin.getSetTopologyAttemptCount();

            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream(inputTopic);
            startStreams(builder.build(), appId);

            // Wait for the first attempt (which will be permanently rejected).
            TestUtils.waitForCondition(
                () -> plugin.getSetTopologyAttemptCount() > attemptsBefore,
                60_000,
                "Plugin never received any setTopology attempt");
            final int attemptsAfterFirstFailure = plugin.getSetTopologyAttemptCount();

            // Wait long enough for several heartbeat cycles (heartbeat interval is seconds-scale).
            // If the broker's hot-loop ratchet works, no further attempts will be made at the same epoch.
            Thread.sleep(15_000);

            final int attemptsLater = plugin.getSetTopologyAttemptCount();
            LOG.info("Push attempts: before-test={}, after-first-failure={}, after-15s={}",
                attemptsBefore, attemptsAfterFirstFailure, attemptsLater);
            assertEquals(attemptsAfterFirstFailure, attemptsLater,
                "Broker re-solicited at the same epoch after a permanent (TooLarge) plugin failure — "
                    + "FailedDescriptionTopologyEpoch ratchet is not preventing the hot loop");
        } finally {
            plugin.setFailOnSetPermanent(false);
        }
    }

    @Test
    public void shouldOnlyPushTopologyOnceWithTwoMembers(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;

        CLUSTER.createTopic(inputTopic, 2, 1);

        final InMemoryTopologyDescriptionPlugin plugin = getPlugin();
        final int countBefore = plugin.getSetTopologyCount();

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);
        final Topology topology = builder.build();

        final KafkaStreams streams1 = new KafkaStreams(topology, streamsProperties(appId));
        streamsInstances.add(streams1);
        final KafkaStreams streams2 = new KafkaStreams(topology, streamsProperties(appId));
        streamsInstances.add(streams2);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(List.of(streams1, streams2));

        final StreamsGroupTopologyDescription pluginTopology = waitForTopology(plugin, appId);
        LOG.info("Topology description received by plugin for group {} with two members: {}",
            appId, pluginTopology);

        // Wait a bit longer to make sure no second push arrives
        Thread.sleep(5_000);

        final int pushCount = plugin.getSetTopologyCount() - countBefore;
        LOG.info("Topology was pushed {} time(s) for group {}", pushCount, appId);
        assertEquals(1, pushCount,
            "Topology should be pushed exactly once even with two members, but was pushed " + pushCount + " times");
    }

    @Test
    public void shouldDeleteTopologyFromPluginOnExplicitGroupDeletion(final TestInfo testInfo) throws Exception {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId-" + testId;
        final String inputTopic = "input-" + testId;
        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);
        startStreams(builder.build(), appId);

        final InMemoryTopologyDescriptionPlugin plugin = getPlugin();
        waitForTopology(plugin, appId);

        // Close streams so the group becomes empty.
        // tearDown() will attempt close again; that is safe because close() is idempotent.
        final KafkaStreams streams = streamsInstances.get(streamsInstances.size() - 1);
        streams.close(Duration.ofSeconds(30));

        // Delete the group, retrying while the broker still considers it non-empty.
        try (final Admin admin = CLUSTER.createAdminClient()) {
            TestUtils.waitForCondition(
                () -> {
                    try {
                        admin.deleteConsumerGroups(List.of(appId)).all().get();
                        return true;
                    } catch (final ExecutionException e) {
                        if (e.getCause() instanceof GroupNotEmptyException) {
                            return false;
                        }
                        throw new RuntimeException(e);
                    }
                },
                60_000,
                "Failed to delete group " + appId + " — broker still reports it non-empty"
            );
        }

        TestUtils.waitForCondition(
            () -> plugin.storedTopology(appId).isEmpty(),
            60_000,
            "Plugin did not remove topology for group " + appId + " after explicit DeleteGroups"
        );
    }

}
