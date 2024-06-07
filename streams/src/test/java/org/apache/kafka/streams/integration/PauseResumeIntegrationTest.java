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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getTopicSize;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilStreamsHasPolled;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Tag("integration")
public class PauseResumeIntegrationTest {
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(45);
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static Properties producerConfig;
    private static Properties consumerConfig;

    private static final Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> IN_MEMORY_STORE =
        Materialized.as(Stores.inMemoryKeyValueStore("store"));

    private static final String INPUT_STREAM_1 = "input-stream-1";
    private static final String INPUT_STREAM_2 = "input-stream-2";
    private static final String OUTPUT_STREAM_1 = "output-stream-1";
    private static final String OUTPUT_STREAM_2 = "output-stream-2";
    private static final String TOPOLOGY1 = "topology1";
    private static final String TOPOLOGY2 = "topology2";

    private static final List<KeyValue<String, Long>> STANDARD_INPUT_DATA =
        asList(pair("A", 100L), pair("B", 200L), pair("A", 300L), pair("C", 400L), pair("C", -50L));
    private static final List<KeyValue<String, Long>> COUNT_OUTPUT_DATA =
        asList(pair("A", 1L), pair("B", 1L), pair("A", 2L), pair("C", 1L), pair("C", 2L));
    private static final List<KeyValue<String, Long>> COUNT_OUTPUT_DATA2 =
        asList(pair("A", 3L), pair("B", 2L), pair("A", 4L), pair("C", 3L), pair("C", 4L));
    private static final List<KeyValue<String, Long>> COUNT_OUTPUT_DATA_ALL = new ArrayList<KeyValue<String, Long>>() {{
            addAll(COUNT_OUTPUT_DATA);
            addAll(COUNT_OUTPUT_DATA2);
        }};

    private String appId;
    private KafkaStreams kafkaStreams, kafkaStreams2;
    private KafkaStreamsNamedTopologyWrapper streamsNamedTopologyWrapper;


    private static Stream<Boolean> parameters() {
        return Stream.of(
            Boolean.TRUE,
            Boolean.FALSE);
    }

    @BeforeAll
    public static void startCluster() throws Exception {
        CLUSTER.start();
        producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(),
            StringSerializer.class, LongSerializer.class);
        consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(),
            StringDeserializer.class, LongDeserializer.class);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws InterruptedException {
        cleanStateBeforeTest(CLUSTER, 1, INPUT_STREAM_1, INPUT_STREAM_2, OUTPUT_STREAM_1, OUTPUT_STREAM_2);
        appId = safeUniqueTestName(testInfo);
    }

    private Properties props(final boolean stateUpdaterEnabled) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        properties.put(StreamsConfig.InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled);
        return properties;
    }

    @AfterEach
    public void shutdown() throws InterruptedException {
        for (final KafkaStreams streams : Arrays.asList(kafkaStreams, kafkaStreams2, streamsNamedTopologyWrapper)) {
            if (streams != null) {
                streams.close(Duration.ofSeconds(30));
            }
        }
    }

    private static void produceToInputTopics(final String topic, final Collection<KeyValue<String, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, producerConfig, CLUSTER.time);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldPauseAndResumeKafkaStreams(final boolean stateUpdaterEnabled) throws Exception {
        kafkaStreams = buildKafkaStreams(OUTPUT_STREAM_1, stateUpdaterEnabled);
        kafkaStreams.start();
        waitForApplicationState(singletonList(kafkaStreams), State.RUNNING, STARTUP_TIMEOUT);

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);

        kafkaStreams.pause();
        assertTrue(kafkaStreams.isPaused());

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        waitUntilStreamsHasPolled(kafkaStreams, 2);
        assertTopicSize(OUTPUT_STREAM_1, 5);

        kafkaStreams.resume();
        assertFalse(kafkaStreams.isPaused());

        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA2);
        assertTopicSize(OUTPUT_STREAM_1, 10);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldAllowForTopologiesToStartPaused(final boolean stateUpdaterEnabled) throws Exception {
        kafkaStreams = buildKafkaStreams(OUTPUT_STREAM_1, stateUpdaterEnabled);
        kafkaStreams.pause();
        kafkaStreams.start();
        waitForApplicationState(singletonList(kafkaStreams), State.REBALANCING, STARTUP_TIMEOUT);
        assertTrue(kafkaStreams.isPaused());

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        waitUntilStreamsHasPolled(kafkaStreams, 2);

        assertTopicSize(OUTPUT_STREAM_1, 0);

        kafkaStreams.resume();
        assertFalse(kafkaStreams.isPaused());
        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);
        assertTopicSize(OUTPUT_STREAM_1, 5);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldPauseAndResumeKafkaStreamsWithNamedTopologies(final boolean stateUpdaterEnabled) throws Exception {
        streamsNamedTopologyWrapper = new KafkaStreamsNamedTopologyWrapper(props(stateUpdaterEnabled));
        final NamedTopologyBuilder builder1 = getNamedTopologyBuilder1();
        final NamedTopologyBuilder builder2 = getNamedTopologyBuilder2();

        streamsNamedTopologyWrapper.start(asList(builder1.build(), builder2.build()));
        waitForApplicationState(singletonList(streamsNamedTopologyWrapper), State.RUNNING, STARTUP_TIMEOUT);

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);
        awaitOutput(OUTPUT_STREAM_2, 5, COUNT_OUTPUT_DATA);
        assertTopicSize(OUTPUT_STREAM_1, 5);
        assertTopicSize(OUTPUT_STREAM_2, 5);

        streamsNamedTopologyWrapper.pauseNamedTopology(TOPOLOGY1);
        assertTrue(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY1));
        assertFalse(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY2));
        assertFalse(streamsNamedTopologyWrapper.isPaused());

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        awaitOutput(OUTPUT_STREAM_2, 5, COUNT_OUTPUT_DATA2);
        assertTopicSize(OUTPUT_STREAM_1, 5);
        assertTopicSize(OUTPUT_STREAM_2, 10);

        streamsNamedTopologyWrapper.resumeNamedTopology(TOPOLOGY1);
        assertFalse(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY1));
        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA2);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldPauseAndResumeAllKafkaStreamsWithNamedTopologies(final boolean stateUpdaterEnabled) throws Exception {
        streamsNamedTopologyWrapper = new KafkaStreamsNamedTopologyWrapper(props(stateUpdaterEnabled));
        final NamedTopologyBuilder builder1 = getNamedTopologyBuilder1();
        final NamedTopologyBuilder builder2 = getNamedTopologyBuilder2();

        streamsNamedTopologyWrapper.start(asList(builder1.build(), builder2.build()));
        waitForApplicationState(singletonList(streamsNamedTopologyWrapper), State.RUNNING, STARTUP_TIMEOUT);

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);
        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);
        awaitOutput(OUTPUT_STREAM_2, 5, COUNT_OUTPUT_DATA);

        streamsNamedTopologyWrapper.pause();
        assertTrue(streamsNamedTopologyWrapper.isPaused());
        assertTrue(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY1));
        assertTrue(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY2));

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        waitUntilStreamsHasPolled(streamsNamedTopologyWrapper, 2);
        assertTopicSize(OUTPUT_STREAM_1, 5);
        assertTopicSize(OUTPUT_STREAM_2, 5);

        streamsNamedTopologyWrapper.resumeNamedTopology(TOPOLOGY1);
        assertFalse(streamsNamedTopologyWrapper.isPaused());
        assertFalse(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY1));
        assertTrue(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY2));
        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA2);
        assertTopicSize(OUTPUT_STREAM_1, 10);
        assertTopicSize(OUTPUT_STREAM_2, 5);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldAllowForNamedTopologiesToStartPaused(final boolean stateUpdaterEnabled) throws Exception {
        streamsNamedTopologyWrapper = new KafkaStreamsNamedTopologyWrapper(props(stateUpdaterEnabled));
        final NamedTopologyBuilder builder1 = getNamedTopologyBuilder1();
        final NamedTopologyBuilder builder2 = getNamedTopologyBuilder2();

        streamsNamedTopologyWrapper.pauseNamedTopology(TOPOLOGY1);
        streamsNamedTopologyWrapper.start(asList(builder1.build(), builder2.build()));
        waitForApplicationState(singletonList(streamsNamedTopologyWrapper), State.REBALANCING, STARTUP_TIMEOUT);

        assertFalse(streamsNamedTopologyWrapper.isPaused());
        assertTrue(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY1));
        assertFalse(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY2));

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        assertTopicSize(OUTPUT_STREAM_1, 0);
        assertTopicSize(OUTPUT_STREAM_2, 0);

        streamsNamedTopologyWrapper.resumeNamedTopology(TOPOLOGY1);
        assertFalse(streamsNamedTopologyWrapper.isPaused());
        assertFalse(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY1));
        assertFalse(streamsNamedTopologyWrapper.isNamedTopologyPaused(TOPOLOGY2));

        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);
        awaitOutput(OUTPUT_STREAM_2, 5, COUNT_OUTPUT_DATA);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void pauseResumeShouldWorkAcrossInstances(final boolean stateUpdaterEnabled) throws Exception {
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        kafkaStreams = buildKafkaStreams(OUTPUT_STREAM_1, stateUpdaterEnabled);
        kafkaStreams.pause();
        kafkaStreams.start();

        waitForApplicationState(singletonList(kafkaStreams), State.REBALANCING, STARTUP_TIMEOUT);
        assertTrue(kafkaStreams.isPaused());

        kafkaStreams2 = buildKafkaStreams(OUTPUT_STREAM_2, stateUpdaterEnabled);
        kafkaStreams2.pause();
        kafkaStreams2.start();
        waitForApplicationState(singletonList(kafkaStreams2), State.REBALANCING, STARTUP_TIMEOUT);
        assertTrue(kafkaStreams2.isPaused());

        assertTopicSize(OUTPUT_STREAM_1, 0);

        kafkaStreams2.close();
        kafkaStreams2.cleanUp();
        waitForApplicationState(singletonList(kafkaStreams2), State.NOT_RUNNING, STARTUP_TIMEOUT);

        kafkaStreams.resume();
        waitForApplicationState(singletonList(kafkaStreams), State.RUNNING, STARTUP_TIMEOUT);

        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void pausedTopologyShouldNotRestoreStateStores(final boolean stateUpdaterEnabled) throws Exception {
        final Properties properties1 = props(stateUpdaterEnabled);
        properties1.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        final Properties properties2 = props(stateUpdaterEnabled);
        properties2.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        kafkaStreams = buildKafkaStreams(OUTPUT_STREAM_1, properties1);
        kafkaStreams2 = buildKafkaStreams(OUTPUT_STREAM_1, properties2);
        kafkaStreams.start();
        kafkaStreams2.start();

        waitForApplicationState(Arrays.asList(kafkaStreams, kafkaStreams2), State.RUNNING, STARTUP_TIMEOUT);

        awaitOutput(OUTPUT_STREAM_1, 5, COUNT_OUTPUT_DATA);

        kafkaStreams.close();
        kafkaStreams2.close();

        kafkaStreams = buildKafkaStreams(OUTPUT_STREAM_1, properties1);
        kafkaStreams2 = buildKafkaStreams(OUTPUT_STREAM_1, properties2);
        kafkaStreams.cleanUp();
        kafkaStreams2.cleanUp();

        kafkaStreams.pause();
        kafkaStreams2.pause();
        kafkaStreams.start();
        kafkaStreams2.start();

        waitForApplicationState(Arrays.asList(kafkaStreams, kafkaStreams2), State.REBALANCING, STARTUP_TIMEOUT);

        assertStreamsLocalStoreLagStaysConstant(kafkaStreams);
        assertStreamsLocalStoreLagStaysConstant(kafkaStreams2);
    }

    private void assertStreamsLocalStoreLagStaysConstant(final KafkaStreams streams) throws InterruptedException {
        waitForCondition(
            () -> streams.allLocalStorePartitionLags().containsKey("test-store"),
            "Lags for test-store partitions were not found within the timeout!");
        waitUntilStreamsHasPolled(streams, 2);
        final long stateStoreLag1 = streams.allLocalStorePartitionLags().get("test-store").get(0).offsetLag();
        waitUntilStreamsHasPolled(streams, 2);
        final long stateStoreLag2 = streams.allLocalStorePartitionLags().get("test-store").get(0).offsetLag();
        assertTrue(stateStoreLag1 > 0);
        assertEquals(stateStoreLag1, stateStoreLag2);
    }

    private KafkaStreams buildKafkaStreams(final String outputTopic, final boolean stateUpdaterEnabled) {
        return buildKafkaStreams(outputTopic, props(stateUpdaterEnabled));
    }

    private KafkaStreams buildKafkaStreams(final String outputTopic, final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_STREAM_1).groupByKey().count(Materialized.as("test-store")).toStream().to(outputTopic);
        return new KafkaStreams(builder.build(properties), properties);
    }

    private void assertTopicSize(final String topicName, final int size) {
        assertEquals(getTopicSize(consumerConfig, topicName), size);
    }

    private void awaitOutput(final String topicName, final int count, final List<KeyValue<String, Long>> output)
        throws Exception {
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, topicName, count), CoreMatchers.equalTo(output));
    }

    private NamedTopologyBuilder getNamedTopologyBuilder1() {
        final NamedTopologyBuilder builder1 = streamsNamedTopologyWrapper.newNamedTopologyBuilder(TOPOLOGY1);
        builder1.stream(INPUT_STREAM_1).groupByKey().count().toStream().to(OUTPUT_STREAM_1);
        return builder1;
    }

    private NamedTopologyBuilder getNamedTopologyBuilder2() {
        final NamedTopologyBuilder builder2 = streamsNamedTopologyWrapper.newNamedTopologyBuilder(TOPOLOGY2);
        builder2.stream(INPUT_STREAM_2)
            .groupBy((k, v) -> k)
            .count(IN_MEMORY_STORE)
            .toStream()
            .to(OUTPUT_STREAM_2);
        return builder2;
    }
}
