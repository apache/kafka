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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.TrackingStandbyUpdateListener;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.TrackingStateRestoreListener;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForCompletion;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForStandbyCompletion;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class RestoreIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(RestoreIntegrationTest.class);

    private static final Duration RESTORATION_DELAY = Duration.ofSeconds(1);

    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private String appId;
    private String inputStream;

    private final int numberOfKeys = 10000;
    private KafkaStreams kafkaStreams;

    private final List<Properties> streamsConfigurations = new ArrayList<>();

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws InterruptedException {
        appId = safeUniqueTestName(testInfo);
        inputStream = appId + "-input-stream";
        CLUSTER.createTopic(inputStream, 2, 1);
    }

    private Properties props(final boolean stateUpdaterEnabled) {
        return props(mkObjectProperties(mkMap(mkEntry(InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled))));
    }

    private Properties props(final Properties extraProperties) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.putAll(extraProperties);

        streamsConfigurations.add(streamsConfiguration);

        return streamsConfiguration;
    }

    @AfterEach
    public void shutdown() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30));
        }

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfigurations);
        streamsConfigurations.clear();
    }

    private static Stream<Boolean> parameters() {
        return Stream.of(
                Boolean.TRUE,
                Boolean.FALSE);
    }

    @Test
    public void shouldRestoreNullRecord() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();

        final String applicationId = "restoration-test-app";
        final String stateStoreName = "stateStore";
        final String inputTopic = "input";
        final String outputTopic = "output";

        final Properties props = new Properties();

        final Properties streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                applicationId,
                CLUSTER.bootstrapServers(),
                Serdes.IntegerSerde.class.getName(),
                Serdes.BytesSerde.class.getName(),
                props);

        CLUSTER.createTopics(inputTopic);
        CLUSTER.createTopics(outputTopic);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        builder.table(inputTopic, Materialized.<Integer, Bytes>as(
                        Stores.persistentTimestampedKeyValueStore(stateStoreName))
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.Bytes())
                .withCachingDisabled()).toStream().to(outputTopic);

        final Properties producerConfig = TestUtils.producerConfig(
                CLUSTER.bootstrapServers(), IntegerSerializer.class, BytesSerializer.class);

        final List<KeyValue<Integer, Bytes>> initialKeyValues = Arrays.asList(
                KeyValue.pair(3, new Bytes(new byte[]{3})),
                KeyValue.pair(3, null),
                KeyValue.pair(1, new Bytes(new byte[]{1})));

        IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic, initialKeyValues, producerConfig, new MockTime());

        KafkaStreams streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
        streams.start();

        final Properties consumerConfig = TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(), IntegerDeserializer.class, BytesDeserializer.class);

        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                consumerConfig, outputTopic, initialKeyValues);

        // wipe out state store to trigger restore process on restart
        streams.close();
        streams.cleanUp();

        // Restart the stream instance. There should not be exception handling the null
        // value within changelog topic.
        final List<KeyValue<Integer, Bytes>> newKeyValues = Collections
                .singletonList(KeyValue.pair(2, new Bytes(new byte[3])));
        IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic, newKeyValues, producerConfig, new MockTime());
        streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
        streams.start();
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                consumerConfig, outputTopic, newKeyValues);
        streams.close();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreStateFromSourceTopicForReadOnlyStore(final boolean stateUpdaterEnabled) throws Exception {
        final AtomicInteger numReceived = new AtomicInteger(0);
        final Topology topology = new Topology();

        final Properties props = props(stateUpdaterEnabled);

        // restoring from 1000 to 4000 (committed), and then process from 4000 to 5000 on each of the two partitions
        final int offsetLimitDelta = 1000;
        final int offsetCheckpointed = 1000;
        createStateForRestoration(inputStream, 0);
        setCommittedOffset(inputStream, offsetLimitDelta);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true, false);
        // note here the checkpointed offset is the last processed record's offset, so without control message we should write this offset - 1
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(inputStream, 0), (long) offsetCheckpointed - 1));
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 1)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(inputStream, 1), (long) offsetCheckpointed - 1));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        topology.addReadOnlyStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store"),
                new Serdes.IntegerSerde(),
                new Serdes.StringSerde()
            ),
            "readOnlySource",
            new IntegerDeserializer(),
            new StringDeserializer(),
            inputStream,
            "readOnlyProcessor",
            () -> new ReadOnlyStoreProcessor(numReceived, offsetLimitDelta, shutdownLatch)
        );

        kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
            }
        });

        final AtomicLong restored = new AtomicLong(0);
        kafkaStreams.setGlobalStateRestoreListener(new TrackingStateRestoreListener(restored));
        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
        assertThat(restored.get(), equalTo((long) numberOfKeys - offsetLimitDelta * 2 - offsetCheckpointed * 2));

        assertTrue(shutdownLatch.await(30, TimeUnit.SECONDS));
        assertThat(numReceived.get(), equalTo(offsetLimitDelta * 2));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreStateFromSourceTopicForGlobalTable(final boolean stateUpdaterEnabled) throws Exception {
        final AtomicInteger numReceived = new AtomicInteger(0);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties props = props(stateUpdaterEnabled);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        // restoring from 1000 to 4000 (committed), and then process from 4000 to 5000 on each of the two partitions
        final int offsetLimitDelta = 1000;
        final int offsetCheckpointed = 1000;
        createStateForRestoration(inputStream, 0);
        setCommittedOffset(inputStream, offsetLimitDelta);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true, false);
        // note here the checkpointed offset is the last processed record's offset, so without control message we should write this offset - 1
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(inputStream, 0), (long) offsetCheckpointed - 1));
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 1)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(inputStream, 1), (long) offsetCheckpointed - 1));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        builder.table(inputStream, Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as("store").withKeySerde(Serdes.Integer()).withValueSerde(Serdes.Integer()))
                .toStream()
                .foreach((key, value) -> {
                    if (numReceived.incrementAndGet() == offsetLimitDelta * 2) {
                        shutdownLatch.countDown();
                    }
                });

        kafkaStreams = new KafkaStreams(builder.build(props), props);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
            }
        });

        final AtomicLong restored = new AtomicLong(0);
        kafkaStreams.setGlobalStateRestoreListener(new TrackingStateRestoreListener(restored));
        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
        assertThat(restored.get(), equalTo((long) numberOfKeys - offsetLimitDelta * 2 - offsetCheckpointed * 2));

        assertTrue(shutdownLatch.await(30, TimeUnit.SECONDS));
        assertThat(numReceived.get(), equalTo(offsetLimitDelta * 2));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreStateFromChangelogTopic(final boolean stateUpdaterEnabled) throws Exception {
        final String changelog = appId + "-store-changelog";
        CLUSTER.createTopic(changelog, 2, 1);

        final AtomicInteger numReceived = new AtomicInteger(0);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties props = props(stateUpdaterEnabled);

        // restoring from 1000 to 5000, and then process from 5000 to 10000 on each of the two partitions
        final int offsetCheckpointed = 1000;
        createStateForRestoration(changelog, 0);
        createStateForRestoration(inputStream, 10000);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true, false);
        // note here the checkpointed offset is the last processed record's offset, so without control message we should write this offset - 1
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(changelog, 0), (long) offsetCheckpointed - 1));
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 1)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(changelog, 1), (long) offsetCheckpointed - 1));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        builder.table(inputStream, Consumed.with(Serdes.Integer(), Serdes.Integer()), Materialized.as("store"))
                .toStream()
                .foreach((key, value) -> {
                    if (numReceived.incrementAndGet() == numberOfKeys) {
                        shutdownLatch.countDown();
                    }
                });

        kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
            }
        });

        final AtomicLong restored = new AtomicLong(0);
        kafkaStreams.setGlobalStateRestoreListener(new TrackingStateRestoreListener(restored));
        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
        assertThat(restored.get(), equalTo((long) numberOfKeys - 2 * offsetCheckpointed));

        assertTrue(shutdownLatch.await(30, TimeUnit.SECONDS));
        assertThat(numReceived.get(), equalTo(numberOfKeys));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldSuccessfullyStartWhenLoggingDisabled(final boolean stateUpdaterEnabled) throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, Integer> stream = builder.stream(inputStream);
        stream
            .groupByKey()
            .reduce(
                Integer::sum,
                Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as("reduce-store").withLoggingDisabled()
            );

        final CountDownLatch startupLatch = new CountDownLatch(1);
        kafkaStreams = new KafkaStreams(builder.build(), props(stateUpdaterEnabled));
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
            }
        });

        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldProcessDataFromStoresWithLoggingDisabled(final boolean stateUpdaterEnabled) throws InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(inputStream,
                asList(KeyValue.pair(1, 1),
                        KeyValue.pair(2, 2),
                        KeyValue.pair(3, 3)),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                        IntegerSerializer.class,
                        IntegerSerializer.class),
                CLUSTER.time);

        final KeyValueBytesStoreSupplier lruMapSupplier = Stores.lruMap(inputStream, 10);

        final StoreBuilder<KeyValueStore<Integer, Integer>> storeBuilder = new KeyValueStoreBuilder<>(lruMapSupplier,
                Serdes.Integer(),
                Serdes.Integer(),
                CLUSTER.time)
                .withLoggingDisabled();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(storeBuilder);

        final KStream<Integer, Integer> stream = streamsBuilder.stream(inputStream);
        final CountDownLatch processorLatch = new CountDownLatch(3);
        stream.process(() -> new KeyValueStoreProcessor(inputStream, processorLatch), inputStream);

        final Topology topology = streamsBuilder.build();

        kafkaStreams = new KafkaStreams(topology, props(stateUpdaterEnabled));

        final CountDownLatch latch = new CountDownLatch(1);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                latch.countDown();
            }
        });
        kafkaStreams.start();

        latch.await(30, TimeUnit.SECONDS);

        assertTrue(processorLatch.await(30, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRecycleStateFromStandbyTaskPromotedToActiveTaskAndNotRestore(final boolean stateUpdaterEnabled) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
                inputStream,
                Consumed.with(Serdes.Integer(), Serdes.Integer()), Materialized.as(getCloseCountingStore("store"))
        );
        createStateForRestoration(inputStream, 0);

        final Properties props1 = props(stateUpdaterEnabled);
        props1.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props1.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + "-1").getPath());
        purgeLocalStreamsState(props1);
        final KafkaStreams streams1 = new KafkaStreams(builder.build(), props1);

        final Properties props2 = props(stateUpdaterEnabled);
        props2.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props2.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + "-2").getPath());
        purgeLocalStreamsState(props2);
        final KafkaStreams streams2 = new KafkaStreams(builder.build(), props2);

        final Set<KafkaStreams.State> transitionedStates1 = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final Set<KafkaStreams.State> transitionedStates2 = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final TrackingStateRestoreListener restoreListener = new TrackingStateRestoreListener();
        final TrackingStandbyUpdateListener standbyUpdateListener = new TrackingStandbyUpdateListener();
        streams1.setStandbyUpdateListener(standbyUpdateListener);
        streams2.setStandbyUpdateListener(standbyUpdateListener);
        streams1.setGlobalStateRestoreListener(restoreListener);
        streams1.setStateListener((newState, oldState) -> transitionedStates1.add(newState));
        streams2.setStateListener((newState, oldState) -> transitionedStates2.add(newState));

        try {
            startApplicationAndWaitUntilRunning(asList(streams1, streams2), Duration.ofSeconds(60));

            waitForCompletion(streams1, 1, 30 * 1000L);
            waitForCompletion(streams2, 1, 30 * 1000L);
            waitForStandbyCompletion(streams1, 1, 30 * 1000L);
            waitForStandbyCompletion(streams2, 1, 30 * 1000L);
        } catch (final Exception e) {
            streams1.close();
            streams2.close();
        }

        // Sometimes the store happens to have already been closed sometime during startup, so just keep track
        // of where it started and make sure it doesn't happen more times from there
        final int initialStoreCloseCount = CloseCountingInMemoryStore.numStoresClosed();
        final long initialNunRestoredCount = restoreListener.totalNumRestored();

        transitionedStates1.clear();
        transitionedStates2.clear();
        try {
            streams2.close();
            waitForTransitionTo(transitionedStates2, State.NOT_RUNNING, Duration.ofSeconds(60));
            waitForTransitionTo(transitionedStates1, State.REBALANCING, Duration.ofSeconds(60));
            waitForTransitionTo(transitionedStates1, State.RUNNING, Duration.ofSeconds(60));

            waitForCompletion(streams1, 1, 30 * 1000L);
            waitForStandbyCompletion(streams1, 1, 30 * 1000L);

            assertThat(restoreListener.totalNumRestored(), CoreMatchers.equalTo(initialNunRestoredCount));

            // After stopping instance 2 and letting instance 1 take over its tasks, we should have closed just two stores
            // total: the active and standby tasks on instance 2
            assertThat(CloseCountingInMemoryStore.numStoresClosed(), equalTo(initialStoreCloseCount + 2));
        } finally {
            streams1.close();
        }
        waitForTransitionTo(transitionedStates1, State.NOT_RUNNING, Duration.ofSeconds(60));
        if (stateUpdaterEnabled) {
            assertThat(standbyUpdateListener.promotedPartitions.size(), CoreMatchers.equalTo(1));
        }
        assertThat(CloseCountingInMemoryStore.numStoresClosed(), CoreMatchers.equalTo(initialStoreCloseCount + 4));
    }

    @Test
    public void shouldInvokeUserDefinedGlobalStateRestoreListener() throws Exception {
        final String inputTopic = "inputTopic";
        final String outputTopic = "outputTopic";
        CLUSTER.createTopic(inputTopic, 5, 1);
        CLUSTER.createTopic(outputTopic, 5, 1);

        final Map<String, Object> kafkaStreams1Configuration = mkMap(
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks1"),
            mkEntry(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks1"),
            mkEntry(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 5)
        );
        final Map<String, Object> kafkaStreams2Configuration = mkMap(
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2"),
            mkEntry(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2"),
            mkEntry(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 5)
        );

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(EARLIEST))
               .groupByKey()
               .reduce((oldVal, newVal) -> newVal)
               .toStream()
               .to(outputTopic);

        final List<KeyValue<Integer, Integer>> sampleData = IntStream.range(0, 100)
                                                                     .mapToObj(i -> new KeyValue<>(i, i))
                                                                     .collect(Collectors.toList());

        sendEvents(inputTopic, sampleData);

        kafkaStreams = startKafkaStreams(builder, null, kafkaStreams1Configuration);

        validateReceivedMessages(sampleData, outputTopic);

        // Close kafkaStreams1 (with cleanup) and start it again to force the restoration of the state.
        kafkaStreams.close(Duration.ofMillis(IntegrationTestUtils.DEFAULT_TIMEOUT));
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfigurations);

        final TestStateRestoreListener kafkaStreams1StateRestoreListener = new TestStateRestoreListener("ks1", RESTORATION_DELAY);
        kafkaStreams = startKafkaStreams(builder, kafkaStreams1StateRestoreListener, kafkaStreams1Configuration);

        assertTrue(kafkaStreams1StateRestoreListener.awaitUntilRestorationStarts());
        assertTrue(kafkaStreams1StateRestoreListener.awaitUntilBatchRestoredIsCalled());

        // Simulate a new instance joining in the middle of the restoration.
        // When this happens, some of the partitions that kafkaStreams1 was restoring will be migrated to kafkaStreams2,
        // and kafkaStreams1 must call StateRestoreListener#onRestoreSuspended.
        final TestStateRestoreListener kafkaStreams2StateRestoreListener = new TestStateRestoreListener("ks2", RESTORATION_DELAY);

        try (final KafkaStreams kafkaStreams2 = startKafkaStreams(builder,
                                                                  kafkaStreams2StateRestoreListener,
                                                                  kafkaStreams2Configuration)) {

            waitForCondition(() -> State.RUNNING == kafkaStreams2.state(),
                             IntegrationTestUtils.DEFAULT_TIMEOUT,
                             () -> "kafkaStreams2 never transitioned to a RUNNING state.");

            assertTrue(kafkaStreams1StateRestoreListener.awaitUntilRestorationSuspends());

            assertTrue(kafkaStreams2StateRestoreListener.awaitUntilRestorationStarts());

            assertTrue(kafkaStreams1StateRestoreListener.awaitUntilRestorationEnds());
            assertTrue(kafkaStreams2StateRestoreListener.awaitUntilRestorationEnds());
        }
    }

    private void validateReceivedMessages(final List<KeyValue<Integer, Integer>> expectedRecords,
                                          final String outputTopic) throws Exception {
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + appId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            IntegerDeserializer.class.getName()
        );
        consumerProperties.setProperty(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            IntegerDeserializer.class.getName()
        );

        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
            consumerProperties,
            outputTopic,
            expectedRecords
        );
    }

    private KafkaStreams startKafkaStreams(final StreamsBuilder streamsBuilder,
                                           final StateRestoreListener stateRestoreListener,
                                           final Map<String, Object> extraConfiguration) {
        final Properties streamsConfiguration = props(mkObjectProperties(extraConfiguration));
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        kafkaStreams.setGlobalStateRestoreListener(stateRestoreListener);
        kafkaStreams.start();

        return kafkaStreams;
    }

    private static final class TestStateRestoreListener implements StateRestoreListener {
        private final String instanceName;
        private final Duration onBatchRestoredSleepDuration;

        private final CountDownLatch onRestoreStartLatch = new CountDownLatch(1);
        private final CountDownLatch onRestoreEndLatch = new CountDownLatch(1);
        private final CountDownLatch onRestoreSuspendedLatch = new CountDownLatch(1);
        private final CountDownLatch onBatchRestoredLatch = new CountDownLatch(1);

        TestStateRestoreListener(final String instanceName, final Duration onBatchRestoredSleepDuration) {
            this.onBatchRestoredSleepDuration = onBatchRestoredSleepDuration;
            this.instanceName = instanceName;
        }

        boolean awaitUntilRestorationStarts() throws InterruptedException {
            return awaitLatchWithTimeout(onRestoreStartLatch);
        }

        boolean awaitUntilRestorationSuspends() throws InterruptedException {
            return awaitLatchWithTimeout(onRestoreSuspendedLatch);
        }

        boolean awaitUntilRestorationEnds() throws InterruptedException {
            return awaitLatchWithTimeout(onRestoreEndLatch);
        }

        public boolean awaitUntilBatchRestoredIsCalled() throws InterruptedException {
            return awaitLatchWithTimeout(onBatchRestoredLatch);
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            log.info("[{}] called onRestoreStart. topicPartition={}, storeName={}, startingOffset={}, endingOffset={}",
                     instanceName, topicPartition, storeName, startingOffset, endingOffset);
            onRestoreStartLatch.countDown();
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            log.info("[{}] called onBatchRestored. topicPartition={}, storeName={}, batchEndOffset={}, numRestored={}",
                     instanceName, topicPartition, storeName, batchEndOffset, numRestored);
            Utils.sleep(onBatchRestoredSleepDuration.toMillis());
            onBatchRestoredLatch.countDown();
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            log.info("[{}] called onRestoreEnd. topicPartition={}, storeName={}, totalRestored={}",
                     instanceName, topicPartition, storeName, totalRestored);
            onRestoreEndLatch.countDown();
        }

        @Override
        public void onRestoreSuspended(final TopicPartition topicPartition,
                                       final String storeName,
                                       final long totalRestored) {
            log.info("[{}] called onRestoreSuspended. topicPartition={}, storeName={}, totalRestored={}",
                     instanceName, topicPartition, storeName, totalRestored);
            onRestoreSuspendedLatch.countDown();
        }

        private static boolean awaitLatchWithTimeout(final CountDownLatch latch) throws InterruptedException {
            return latch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    private void sendEvents(final String topic, final List<KeyValue<Integer, Integer>> events) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            events,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            CLUSTER.time
        );
    }

    private static KeyValueBytesStoreSupplier getCloseCountingStore(final String name) {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CloseCountingInMemoryStore(name);
            }

            @Override
            public String metricsScope() {
                return "close-counting";
            }
        };
    }

    static class CloseCountingInMemoryStore extends InMemoryKeyValueStore {
        static AtomicInteger numStoresClosed = new AtomicInteger(0);

        CloseCountingInMemoryStore(final String name) {
            super(name);
        }

        @Override
        public void close() {
            numStoresClosed.incrementAndGet();
            super.close();
        }

        static int numStoresClosed() {
            return numStoresClosed.get();
        }
    }

    public static class KeyValueStoreProcessor implements Processor<Integer, Integer, Void, Void> {
        private final String topic;
        private final CountDownLatch processorLatch;

        private KeyValueStore<Integer, Integer> store;

        KeyValueStoreProcessor(final String topic, final CountDownLatch processorLatch) {
            this.topic = topic;
            this.processorLatch = processorLatch;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore(topic);
        }

        @Override
        public void process(final Record<Integer, Integer> record) {
            if (record.key() != null) {
                store.put(record.key(), record.value());
                processorLatch.countDown();
            }
        }
    }

    private void createStateForRestoration(final String changelogTopic, final int startingOffset) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        try (final KafkaProducer<Integer, Integer> producer =
                     new KafkaProducer<>(producerConfig, new IntegerSerializer(), new IntegerSerializer())) {

            for (int i = 0; i < numberOfKeys; i++) {
                final int offset = startingOffset + i;
                producer.send(new ProducerRecord<>(changelogTopic, offset, offset));
            }
        }
    }

    private void setCommittedOffset(final String topic, final int limitDelta) {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "commit-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);

        final Consumer<Integer, Integer> consumer = new KafkaConsumer<>(consumerConfig);
        final List<TopicPartition> partitions = asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1));

        consumer.assign(partitions);
        consumer.seekToEnd(partitions);

        for (final TopicPartition partition : partitions) {
            final long position = consumer.position(partition);
            consumer.seek(partition, position - limitDelta);
        }

        consumer.commitSync();
        consumer.close();
    }

    private void waitForTransitionTo(final Set<KafkaStreams.State> observed, final KafkaStreams.State state, final Duration timeout) throws Exception {
        waitForCondition(
            () -> observed.contains(state),
            timeout.toMillis(),
            () -> "Client did not transition to " + state + " on time. Observed transitions: " + observed
        );
    }

    private static class ReadOnlyStoreProcessor implements Processor<Integer, String, Void, Void> {
        private final AtomicInteger numReceived;
        private final int offsetLimitDelta;
        private final CountDownLatch shutdownLatch;
        KeyValueStore<Integer, String> store;

        public ReadOnlyStoreProcessor(final AtomicInteger numReceived, final int offsetLimitDelta, final CountDownLatch shutdownLatch) {
            this.numReceived = numReceived;
            this.offsetLimitDelta = offsetLimitDelta;
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore("store");
        }

        @Override
        public void process(final Record<Integer, String> record) {
            store.put(record.key(), record.value());
            if (numReceived.incrementAndGet() == offsetLimitDelta * 2) {
                shutdownLatch.countDown();
            }
        }
    }
}