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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.TrackingStateRestoreListener;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.assignment.LegacyStickyTaskAssignor;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyStreamGroup;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Timeout(600)
@Tag("integration")
public class ColdStartStickinessIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 4;
    private static final int NUM_KEYS = 1_000;
    private static final int INITIAL_REBALANCE_DELAY_MS = 5_000;
    private static final Pattern TASK_DIR = Pattern.compile("\\d+_\\d+");

    public static final EmbeddedKafkaCluster CLUSTER;
    static {
        final Properties brokerProps = new Properties();
        brokerProps.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, Integer.toString(INITIAL_REBALANCE_DELAY_MS));
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, Integer.toString(INITIAL_REBALANCE_DELAY_MS));
        CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, brokerProps);
    }

    private static Admin admin;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        final Properties adminConfig = new Properties();
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        admin = Admin.create(adminConfig);
    }

    @AfterAll
    public static void closeCluster() {
        Utils.closeQuietly(admin, "admin");
        CLUSTER.stop();
    }

    private String appId;
    private String inputTopic;
    private String outputTopic;
    private final List<Properties> streamsConfigurations = new ArrayList<>();

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws InterruptedException {
        appId = safeUniqueTestName(testInfo);
        inputTopic = appId + "-input";
        outputTopic = appId + "-output";
        CLUSTER.createTopic(inputTopic, NUM_PARTITIONS, 1);
        CLUSTER.createTopic(outputTopic, NUM_PARTITIONS, 1);
    }

    @AfterEach
    public void cleanup() throws Exception {
        purgeLocalStreamsState(streamsConfigurations);
        streamsConfigurations.clear();
        CLUSTER.deleteAllTopics();
    }

    @Disabled("Reproduces KAFKA-20719; enable once fixed")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldStickToLocalStateOnColdStart(final boolean streamsProtocol) throws Exception {
        final Properties props1 = props(streamsProtocol, "-1");
        final Properties props2 = props(streamsProtocol, "-2");

        // Phase 1: build committed local state for all 8 tasks across the two instances.
        produceInput();
        final KafkaStreams streams1 = new KafkaStreams(topology(), props1);
        final KafkaStreams streams2 = new KafkaStreams(topology(), props2);
        try {
            startApplicationAndWaitUntilRunning(asList(streams1, streams2), Duration.ofSeconds(60));
            waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, NUM_KEYS, 120_000L);
        } finally {
            streams1.close(Duration.ofSeconds(60));
            streams2.close(Duration.ofSeconds(60));
        }

        // Make sure the group is fully drained before restarting, so we get a proper cold start
        if (streamsProtocol) {
            waitForEmptyStreamGroup(admin, appId, 60_000L);
        } else {
            waitForEmptyConsumerGroup(admin, appId, 60_000L);
        }

        // change task layout across both instances, to differ from "empty state" assignment
        final File appDir1 = new File(props1.getProperty(StreamsConfig.STATE_DIR_CONFIG), appId);
        final File appDir2 = new File(props2.getProperty(StreamsConfig.STATE_DIR_CONFIG), appId);
        relayoutTaskDirectoriesByPartitionHalves(appDir1, appDir2);

        // Phase 2: cold restart: should be sticky and not restore anything
        final TrackingStateRestoreListener restore1 = new TrackingStateRestoreListener();
        final TrackingStateRestoreListener restore2 = new TrackingStateRestoreListener();
        final KafkaStreams restarted1 = new KafkaStreams(topology(), props1);
        final KafkaStreams restarted2 = new KafkaStreams(topology(), props2);
        restarted1.setGlobalStateRestoreListener(restore1);
        restarted2.setGlobalStateRestoreListener(restore2);
        try {
            startApplicationAndWaitUntilRunning(asList(restarted1, restarted2), Duration.ofSeconds(60));

            final long totalRestored = restore1.totalNumRestored() + restore2.totalNumRestored();
            assertThat(
                "Tasks should be assigned to the instance that already holds their state, so nothing is restored",
                totalRestored,
                equalTo(0L));
        } finally {
            restarted1.close(Duration.ofSeconds(60));
            restarted2.close(Duration.ofSeconds(60));
        }
    }

    private Properties props(final boolean streamsProtocol, final String stateDirSuffix) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + stateDirSuffix).getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        if (streamsProtocol) {
            config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        } else {
            config.put(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, LegacyStickyTaskAssignor.class.getName());
        }
        streamsConfigurations.add(config);
        return config;
    }

    private Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .table(
                inputTopic,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                Materialized.<Integer, Integer>as(Stores.persistentKeyValueStore("store"))
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.Integer())
                    .withCachingDisabled())
            .toStream()
            .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Integer()));
        return builder.build();
    }

    private void produceInput() {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        final List<KeyValue<Integer, Integer>> data = IntStream.range(0, NUM_KEYS)
            .mapToObj(i -> KeyValue.pair(i, i))
            .collect(Collectors.toList());
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, data, producerConfig, CLUSTER.time);
    }

    private Properties consumerConfig() {
        final Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, appId + "-verifier");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return config;
    }

    /*
     * Sticky-assignor (both "classic" and "streams") use a "least loaded greedy" strategy,
     * resulting in the following layout (task are sorted and assigned):
     *  - instance A: 0_0, 0_2
     *  - instance B: 0_1, 0_3
     *
     * We change the layout to
     *  - instance A: 0_0, 0_1
     *  - instance B: 0_2, 0_3
     */
    private static void relayoutTaskDirectoriesByPartitionHalves(final File appDir1, final File appDir2) throws IOException {
        final List<File> taskDirs = new ArrayList<>();
        taskDirs.addAll(listTaskDirectories(appDir1));
        taskDirs.addAll(listTaskDirectories(appDir2));
        assertThat("expected one state directory per task", taskDirs.size(), equalTo(NUM_PARTITIONS));

        for (final File taskDir : taskDirs) {
            final int partition = Integer.parseInt(taskDir.getName().substring(taskDir.getName().indexOf('_') + 1));
            final File target = partition < NUM_PARTITIONS / 2 ? appDir1 : appDir2;
            if (!taskDir.getParentFile().equals(target)) {
                Files.move(taskDir.toPath(), new File(target, taskDir.getName()).toPath());
            }
        }
    }

    private static List<File> listTaskDirectories(final File appDir) {
        final File[] dirs = appDir.listFiles(file -> file.isDirectory() && TASK_DIR.matcher(file.getName()).matches());
        return dirs == null ? List.of() : Arrays.asList(dirs);
    }
}
