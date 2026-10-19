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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.CloseOptions.GroupMembershipOperation;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getRunningStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.sessionStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class QueryableStateIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(QueryableStateIntegrationTest.class);

    private static final long DEFAULT_TIMEOUT_MS = 120 * 1000;

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

    private static final int STREAM_THREE_PARTITIONS = 4;
    private final MockTime mockTime = CLUSTER.time;
    private String streamOne = "stream-one";
    private String streamTwo = "stream-two";
    private String streamThree = "stream-three";
    private String streamConcurrent = "stream-concurrent";
    private String outputTopic = "output";
    private String outputTopicConcurrent = "output-concurrent";
    private String outputTopicConcurrentWindowed = "output-concurrent-windowed";
    private String outputTopicThree = "output-three";
    // sufficiently large window size such that everything falls into 1 window
    private static final long WINDOW_SIZE = TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS);
    // stores of the Processor API topology built by headersStoresTopology()
    private static final String HEADERS_KV_STORE = "headers-kv-store";
    private static final String HEADERS_WINDOW_STORE = "headers-window-store";
    private static final String HEADERS_SESSION_STORE = "headers-session-store";
    private static final long HEADERS_WINDOW_SIZE_MS = 10_000L;
    private static final int STREAM_TWO_PARTITIONS = 2;
    private static final int NUM_REPLICAS = NUM_BROKERS;
    private Properties streamsConfiguration;
    private List<String> inputValues;
    private Set<String> inputValuesKeys;
    private KafkaStreams kafkaStreams;
    private Comparator<KeyValue<String, String>> stringComparator;
    private Comparator<KeyValue<String, Long>> stringLongComparator;

    private void createTopics(final String safeTestName) throws Exception {
        streamOne = streamOne + "-" + safeTestName;
        streamConcurrent = streamConcurrent + "-" + safeTestName;
        streamThree = streamThree + "-" + safeTestName;
        outputTopic = outputTopic + "-" + safeTestName;
        outputTopicConcurrent = outputTopicConcurrent + "-" + safeTestName;
        outputTopicConcurrentWindowed = outputTopicConcurrentWindowed + "-" + safeTestName;
        outputTopicThree = outputTopicThree + "-" + safeTestName;
        streamTwo = streamTwo + "-" + safeTestName;
        CLUSTER.createTopics(streamOne, streamConcurrent);
        CLUSTER.createTopic(streamTwo, STREAM_TWO_PARTITIONS, NUM_REPLICAS);
        CLUSTER.createTopic(streamThree, STREAM_THREE_PARTITIONS, 1);
        CLUSTER.createTopics(outputTopic, outputTopicConcurrent, outputTopicConcurrentWindowed, outputTopicThree);
    }

    /**
     * Try to read inputValues from {@code resources/QueryableStateIntegrationTest/inputValues.txt}, which might be useful
     * for larger scale testing. In case of exception, for instance if no such file can be read, return a small list
     * which satisfies all the prerequisites of the tests.
     */
    private List<String> getInputValues() {
        List<String> input = new ArrayList<>();
        final ClassLoader classLoader = getClass().getClassLoader();
        final String fileName = "QueryableStateIntegrationTest" + File.separator + "inputValues.txt";
        try (final BufferedReader reader = new BufferedReader(
            new FileReader(Objects.requireNonNull(classLoader.getResource(fileName)).getFile()))) {

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                input.add(line);
            }
        } catch (final Exception e) {
            log.warn("Unable to read '{}{}{}'. Using default inputValues list", "resources", File.separator, fileName);
            input = Arrays.asList(
                        "hello world",
                        "all streams lead to kafka",
                        "streams",
                        "kafka streams",
                        "the cat in the hat",
                        "green eggs and ham",
                        "that Sam i am",
                        "up the creek without a paddle",
                        "run forest run",
                        "a tank full of gas",
                        "eat sleep rave repeat",
                        "one jolly sailor",
                        "king of the world");

        }
        return input;
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        final String safeTestName = safeUniqueTestName(testInfo);
        createTopics(safeTestName);
        streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        stringComparator = Comparator.comparing((KeyValue<String, String> o) -> o.key).thenComparing(o -> o.value);
        stringLongComparator = Comparator.comparing((KeyValue<String, Long> o) -> o.key).thenComparingLong(o -> o.value);
        inputValues = getInputValues();
        inputValuesKeys = new HashSet<>();
        for (final String sentence : inputValues) {
            final String[] words = sentence.split("\\W+");
            Collections.addAll(inputValuesKeys, words);
        }
    }

    @AfterEach
    public void shutdown() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close(ofSeconds(30));
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        CLUSTER.deleteAllTopics();
    }

    /**
     * Creates a typical word count topology
     */
    private KafkaStreams createCountStream(final String inputTopic,
                                           final String outputTopic,
                                           final String windowOutputTopic,
                                           final String storeName,
                                           final String windowStoreName,
                                           final Properties streamsConfiguration) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, String> textLines = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));

        final KGroupedStream<String, String> groupedByWord = textLines
            .flatMapValues((ValueMapper<String, Iterable<String>>) value -> Arrays.asList(value.split("\\W+")))
            .groupBy(MockMapper.selectValueMapper());

        // Create a State Store for the all time word count
        groupedByWord
            .count(Materialized.as(storeName + "-" + inputTopic))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        // Create a Windowed State Store that contains the word count for every 1 minute
        groupedByWord
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(WINDOW_SIZE)))
            .count(Materialized.as(windowStoreName + "-" + inputTopic))
            .toStream((key, value) -> key.key())
            .to(windowOutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }


    private void verifyOffsetLagFetch(final List<KafkaStreams> streamsList,
        final Set<String> stores,
        final List<Integer> partitionsPerStreamsInstance) {
        for (int i = 0; i < streamsList.size(); i++) {
            final Map<String, Map<Integer, LagInfo>> localLags = streamsList.get(i).allLocalStorePartitionLags();
            final int expectedPartitions = partitionsPerStreamsInstance.get(i);
            assertEquals(expectedPartitions, localLags.values().stream().mapToInt(Map::size).sum());
            if (expectedPartitions > 0) {
                assertEquals(stores, localLags.keySet());
            }
        }
    }

    private void verifyAllKVKeys(final List<KafkaStreams> streamsList,
                                 final KafkaStreams streams,
                                 final StateListenerStub stateListener,
                                 final Set<String> keys,
                                 final String storeName,
                                 final long timeout,
                                 final boolean pickInstanceByPort) throws Exception {
        retryOnExceptionWithTimeout(timeout, () -> {
            final List<String> noMetadataKeys = new ArrayList<>();
            final List<String> nullStoreKeys = new ArrayList<>();
            final List<String> nullValueKeys = new ArrayList<>();
            final Map<String, Exception> exceptionalKeys = new TreeMap<>();
            final StringSerializer serializer = new StringSerializer();

            for (final String key: keys) {
                try {
                    final KeyQueryMetadata queryMetadata = streams.queryMetadataForKey(storeName, key, serializer);
                    if (queryMetadata == null || queryMetadata.equals(KeyQueryMetadata.NOT_AVAILABLE)) {
                        noMetadataKeys.add(key);
                        continue;
                    }
                    if (!pickInstanceByPort) {
                        assertFalse(queryMetadata.standbyHosts().isEmpty(), "Should have standbys to query from");
                    }

                    final int index = queryMetadata.activeHost().port();
                    final KafkaStreams streamsWithKey = pickInstanceByPort ? streamsList.get(index) : streams;
                    final ReadOnlyKeyValueStore<String, Long> store =
                        IntegrationTestUtils.getStore(storeName, streamsWithKey, true, keyValueStore());
                    if (store == null) {
                        nullStoreKeys.add(key);
                        continue;
                    }

                    if (store.get(key) == null) {
                        nullValueKeys.add(key);
                    }
                } catch (final InvalidStateStoreException e) {
                    if (stateListener.mapStates.get(KafkaStreams.State.REBALANCING) < 1) {
                        throw new NoRetryException(new AssertionError(
                            String.format("Received %s for key %s and expected at least one rebalancing state, but had none",
                                e.getClass().getName(), key)));
                    }
                } catch (final Exception e) {
                    exceptionalKeys.put(key, e);
                }
            }

            assertNoKVKeyFailures(storeName, timeout, noMetadataKeys, nullStoreKeys, nullValueKeys, exceptionalKeys);
        });
    }

    private void verifyAllWindowedKeys(final List<KafkaStreams> streamsList,
                                       final KafkaStreams streams,
                                       final StateListenerStub stateListenerStub,
                                       final Set<String> keys,
                                       final String storeName,
                                       final Long from,
                                       final Long to,
                                       final long timeout,
                                       final boolean pickInstanceByPort) throws Exception {
        retryOnExceptionWithTimeout(timeout, () -> {
            final List<String> noMetadataKeys = new ArrayList<>();
            final List<String> nullStoreKeys = new ArrayList<>();
            final List<String> nullValueKeys = new ArrayList<>();
            final Map<String, Exception> exceptionalKeys = new TreeMap<>();
            final StringSerializer serializer = new StringSerializer();

            for (final String key: keys) {
                try {
                    final KeyQueryMetadata queryMetadata = streams.queryMetadataForKey(storeName, key, serializer);
                    if (queryMetadata == null || queryMetadata.equals(KeyQueryMetadata.NOT_AVAILABLE)) {
                        noMetadataKeys.add(key);
                        continue;
                    }
                    if (pickInstanceByPort) {
                        assertEquals(0, queryMetadata.standbyHosts().size());
                    } else {
                        assertFalse(queryMetadata.standbyHosts().isEmpty(), "Should have standbys to query from");
                    }

                    final int index = queryMetadata.activeHost().port();
                    final KafkaStreams streamsWithKey = pickInstanceByPort ? streamsList.get(index) : streams;
                    final ReadOnlyWindowStore<String, Long> store =
                        IntegrationTestUtils.getStore(storeName, streamsWithKey, true, QueryableStoreTypes.windowStore());
                    if (store == null) {
                        nullStoreKeys.add(key);
                        continue;
                    }

                    if (store.fetch(key, ofEpochMilli(from), ofEpochMilli(to)) == null) {
                        nullValueKeys.add(key);
                    }
                } catch (final InvalidStateStoreException e) {
                    // there must have been at least one rebalance state
                    if (stateListenerStub.mapStates.get(KafkaStreams.State.REBALANCING) < 1) {
                        throw new NoRetryException(new AssertionError(
                            String.format("Received %s for key %s and expected at least one rebalancing state, but had none",
                                e.getClass().getName(), key)));
                    }
                } catch (final Exception e) {
                    exceptionalKeys.put(key, e);
                }
            }

            assertNoKVKeyFailures(storeName, timeout, noMetadataKeys, nullStoreKeys, nullValueKeys, exceptionalKeys);
        });
    }

    private void assertNoKVKeyFailures(final String storeName,
                                       final long timeout,
                                       final List<String> noMetadataKeys,
                                       final List<String> nullStoreKeys,
                                       final List<String> nullValueKeys,
                                       final Map<String, Exception> exceptionalKeys) throws IOException {
        final StringBuilder reason = new StringBuilder();
        reason.append(String.format("Not all keys are available for store %s in %d ms", storeName, timeout));
        if (!noMetadataKeys.isEmpty()) {
            reason.append("\n    * No metadata is available for these keys: ").append(noMetadataKeys);
        }
        if (!nullStoreKeys.isEmpty()) {
            reason.append("\n    * No store is available for these keys: ").append(nullStoreKeys);
        }
        if (!nullValueKeys.isEmpty()) {
            reason.append("\n    * No value is available for these keys: ").append(nullValueKeys);
        }
        if (!exceptionalKeys.isEmpty()) {
            reason.append("\n    * Exceptions were raised for the following keys: ");
            for (final Entry<String, Exception> entry : exceptionalKeys.entrySet()) {
                reason.append(String.format("\n        %s:", entry.getKey()));

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final Exception exception = entry.getValue();

                exception.printStackTrace(new PrintStream(baos));
                try (final BufferedReader reader = new BufferedReader(new StringReader(baos.toString()))) {
                    String line = reader.readLine();
                    while (line != null) {
                        reason.append("\n            ").append(line);
                        line = reader.readLine();
                    }
                }
            }
        }

        assertTrue(
            noMetadataKeys.isEmpty() && nullStoreKeys.isEmpty() && nullValueKeys.isEmpty() && exceptionalKeys.isEmpty(),
            reason.toString());
    }

    @Test
    public void shouldRejectNonExistentStoreName(final TestInfo testInfo) {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-input-table";

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            input,
            Materialized
                .<String, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        CLUSTER.createTopic(input);

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            final ReadOnlyKeyValueStore<String, String> store =
                streams.store(fromNameAndType(storeName, keyValueStore()));
            assertNotNull(store);

            final UnknownStateStoreException exception = assertThrows(
                UnknownStateStoreException.class,
                () -> streams.store(fromNameAndType("no-table", keyValueStore()))
            );
            assertEquals(
                "Cannot get state store no-table because no such store is registered in the topology.",
                exception.getMessage()
            );
        }
    }

    @Test
    public void shouldRejectWronglyTypedStore(final TestInfo testInfo) {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-input-table";

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            input,
            Materialized
                .<String, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        CLUSTER.createTopic(input);

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            final ReadOnlyKeyValueStore<String, String> store =
                streams.store(fromNameAndType(storeName, keyValueStore()));
            assertNotNull(store);

            // Note that to check the type we actually need a store reference,
            // so we can't check when you get the IQ store, only when you
            // try to use it. Presumably, this could be improved.
            final ReadOnlySessionStore<String, String> sessionStore =
                streams.store(fromNameAndType(storeName, sessionStore()));
            final InvalidStateStoreException exception = assertThrows(
                InvalidStateStoreException.class,
                () -> sessionStore.fetch("a")
            );
            assertEquals(
                "Cannot get state store " + storeName + " because the queryable store type" +
                    " [class org.apache.kafka.streams.state.QueryableStoreTypes$SessionStoreType]" +
                    " does not accept the actual store type" +
                    " [class org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore].",
                exception.getMessage()
            );
        }
    }

    @Test
    public void shouldBeAbleToQueryDuringRebalance() throws Exception {
        final int numThreads = STREAM_TWO_PARTITIONS;
        final List<KafkaStreams> streamsList = new ArrayList<>(numThreads);
        final List<StateListenerStub> listeners = new ArrayList<>(numThreads);

        final ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, 1);
        producerRunnable.run();

        // create stream threads
        final String storeName = "word-count-store";
        final String windowStoreName = "windowed-word-count-store";
        for (int i = 0; i < numThreads; i++) {
            final Properties props = (Properties) streamsConfiguration.clone();
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("shouldBeAbleToQueryDuringRebalance-" + i).getPath());
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + i);
            props.put(StreamsConfig.CLIENT_ID_CONFIG, "instance-" + i);
            final KafkaStreams streams =
                createCountStream(streamThree, outputTopicThree, outputTopicConcurrentWindowed, storeName, windowStoreName, props);
            final StateListenerStub listener = new StateListenerStub();
            streams.setStateListener(listener);
            listeners.add(listener);
            streamsList.add(streams);
        }

        try {
            startApplicationAndWaitUntilRunning(streamsList, Duration.ofSeconds(60));

            final Set<String> stores = Set.of(storeName + "-" + streamThree, windowStoreName + "-" + streamThree);
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(4, 4));

            waitUntilAtLeastNumRecordProcessed(outputTopicThree, 1);

            for (int i = 0; i < streamsList.size(); i++) {
                verifyAllKVKeys(
                    streamsList,
                    streamsList.get(i),
                    listeners.get(i),
                    inputValuesKeys,
                    storeName + "-" + streamThree,
                    DEFAULT_TIMEOUT_MS,
                    true);
                verifyAllWindowedKeys(
                    streamsList,
                    streamsList.get(i),
                    listeners.get(i),
                    inputValuesKeys,
                    windowStoreName + "-" + streamThree,
                    0L,
                    WINDOW_SIZE,
                    DEFAULT_TIMEOUT_MS,
                    true);
            }
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(4, 4));

            // kill N-1 threads
            for (int i = 1; i < streamsList.size(); i++) {
                final Duration closeTimeout = Duration.ofSeconds(60);
                assertTrue(streamsList.get(i).close(closeTimeout),
                    String.format("Streams instance %s did not close in %d ms", i, closeTimeout.toMillis()));
            }

            waitForApplicationState(streamsList.subList(1, numThreads), State.NOT_RUNNING, Duration.ofSeconds(60));
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(4, 0));

            // It's not enough to assert that the first instance is RUNNING because it is possible
            // for the above checks to succeed while the instance is in a REBALANCING state.
            waitForApplicationState(streamsList.subList(0, 1), State.RUNNING, Duration.ofSeconds(60));
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(4, 0));

            // Even though the closed instance(s) are now in NOT_RUNNING there is no guarantee that
            // the running instance is aware of this, so we must run our follow up queries with
            // enough time for the shutdown to be detected.

            // query from the remaining thread
            verifyAllKVKeys(
                streamsList,
                streamsList.get(0),
                listeners.get(0),
                inputValuesKeys,
                storeName + "-" + streamThree,
                DEFAULT_TIMEOUT_MS,
                true);
            verifyAllWindowedKeys(
                streamsList,
                streamsList.get(0),
                listeners.get(0),
                inputValuesKeys,
                windowStoreName + "-" + streamThree,
                0L,
                WINDOW_SIZE,
                DEFAULT_TIMEOUT_MS,
                true);
            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> verifyOffsetLagFetch(streamsList, stores, Arrays.asList(8, 0)));
        } finally {
            for (final KafkaStreams streams : streamsList) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldBeAbleQueryStandbyStateDuringRebalance() throws Exception {
        final int numThreads = STREAM_TWO_PARTITIONS;
        final List<KafkaStreams> streamsList = new ArrayList<>(numThreads);
        final List<StateListenerStub> listeners = new ArrayList<>(numThreads);

        final ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, 1);
        producerRunnable.run();

        // create stream threads
        final String storeName = "word-count-store";
        final String windowStoreName = "windowed-word-count-store";
        final Set<String> stores = Set.of(storeName + "-" + streamThree, windowStoreName + "-" + streamThree);
        for (int i = 0; i < numThreads; i++) {
            final Properties props = (Properties) streamsConfiguration.clone();
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + i);
            props.put(StreamsConfig.CLIENT_ID_CONFIG, "instance-" + i);
            props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("shouldBeAbleQueryStandbyStateDuringRebalance-" + i).getPath());
            final KafkaStreams streams =
                createCountStream(streamThree, outputTopicThree, outputTopicConcurrentWindowed, storeName, windowStoreName, props);
            final StateListenerStub listener = new StateListenerStub();
            streams.setStateListener(listener);
            listeners.add(listener);
            streamsList.add(streams);
        }

        try {
            startApplicationAndWaitUntilRunning(streamsList, ofSeconds(60));
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(8, 8));

            waitUntilAtLeastNumRecordProcessed(outputTopicThree, 1);

            // Ensure each thread can serve all keys by itself; i.e standby replication works.
            for (int i = 0; i < streamsList.size(); i++) {
                verifyAllKVKeys(
                    streamsList,
                    streamsList.get(i),
                    listeners.get(i),
                    inputValuesKeys,
                    storeName + "-" + streamThree,
                    DEFAULT_TIMEOUT_MS,
                    false);
                verifyAllWindowedKeys(
                    streamsList,
                    streamsList.get(i),
                    listeners.get(i),
                    inputValuesKeys,
                    windowStoreName + "-" + streamThree,
                    0L,
                    WINDOW_SIZE,
                    DEFAULT_TIMEOUT_MS,
                    false);
            }
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(8, 8));

            // kill N-1 threads
            for (int i = 1; i < streamsList.size(); i++) {
                final Duration closeTimeout = Duration.ofSeconds(60);
                assertTrue(streamsList.get(i).close(closeTimeout),
                    String.format("Streams instance %s did not close in %d ms", i, closeTimeout.toMillis()));
            }

            waitForApplicationState(streamsList.subList(1, numThreads), State.NOT_RUNNING, Duration.ofSeconds(60));
            verifyOffsetLagFetch(streamsList, stores, Arrays.asList(8, 0));

            // Now, confirm that all the keys are still queryable on the remaining thread, regardless of the state
            verifyAllKVKeys(
                streamsList,
                streamsList.get(0),
                listeners.get(0),
                inputValuesKeys,
                storeName + "-" + streamThree,
                DEFAULT_TIMEOUT_MS,
                false);
            verifyAllWindowedKeys(
                streamsList,
                streamsList.get(0),
                listeners.get(0),
                inputValuesKeys,
                windowStoreName + "-" + streamThree,
                0L,
                WINDOW_SIZE,
                DEFAULT_TIMEOUT_MS,
                false);

            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> verifyOffsetLagFetch(streamsList, stores, Arrays.asList(8, 0)));
        } finally {
            for (final KafkaStreams streams : streamsList) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldBeAbleToQueryStateWithZeroSizedCache() throws Exception {
        verifyCanQueryState(0);
    }

    @Test
    public void shouldBeAbleToQueryStateWithNonZeroSizedCache() throws Exception {
        verifyCanQueryState(10 * 1024 * 1024);
    }

    @Test
    public void shouldBeAbleToQueryFilterState() throws Exception {
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        final Set<KeyValue<String, Long>> batch1 = new HashSet<>(
            Arrays.asList(
                new KeyValue<>(keys[0], 1L),
                new KeyValue<>(keys[1], 1L),
                new KeyValue<>(keys[2], 3L),
                new KeyValue<>(keys[3], 5L),
                new KeyValue<>(keys[4], 2L))
        );
        final Set<KeyValue<String, Long>> expectedBatch1 =
            Set.of(new KeyValue<>(keys[4], 2L));

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                LongSerializer.class,
                new Properties()),
            mockTime);
        final Predicate<String, Long> filterPredicate = (key, value) -> key.contains("kafka");
        final KTable<String, Long> t1 = builder.table(streamOne);
        final KTable<String, Long> t2 = t1.filter(filterPredicate, Materialized.as("queryFilter"));
        t1.filterNot(filterPredicate, Materialized.as("queryFilterNot"));
        t2.toStream().to(outputTopic);

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        waitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        final ReadOnlyKeyValueStore<String, Long> myFilterStore =
            IntegrationTestUtils.getStore("queryFilter", kafkaStreams, keyValueStore());

        final ReadOnlyKeyValueStore<String, Long> myFilterNotStore =
            IntegrationTestUtils.getStore("queryFilterNot", kafkaStreams, keyValueStore());

        for (final KeyValue<String, Long> expectedEntry : expectedBatch1) {
            TestUtils.waitForCondition(() -> expectedEntry.value.equals(myFilterStore.get(expectedEntry.key)),
                    "Cannot get expected result");
        }
        for (final KeyValue<String, Long> batchEntry : batch1) {
            if (!expectedBatch1.contains(batchEntry)) {
                TestUtils.waitForCondition(() -> myFilterStore.get(batchEntry.key) == null,
                        "Cannot get null result");
            }
        }

        for (final KeyValue<String, Long> expectedEntry : expectedBatch1) {
            TestUtils.waitForCondition(() -> myFilterNotStore.get(expectedEntry.key) == null,
                    "Cannot get null result");
        }
        for (final KeyValue<String, Long> batchEntry : batch1) {
            if (!expectedBatch1.contains(batchEntry)) {
                TestUtils.waitForCondition(() -> batchEntry.value.equals(myFilterNotStore.get(batchEntry.key)),
                        "Cannot get expected result");
            }
        }
    }

    @Test
    public void shouldBeAbleToQueryMapValuesState() throws Exception {
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        final Set<KeyValue<String, String>> batch1 = new HashSet<>(
            Arrays.asList(
                new KeyValue<>(keys[0], "1"),
                new KeyValue<>(keys[1], "1"),
                new KeyValue<>(keys[2], "3"),
                new KeyValue<>(keys[3], "5"),
                new KeyValue<>(keys[4], "2"))
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);

        final KTable<String, String> t1 = builder.table(streamOne);
        t1
            .mapValues(
                (ValueMapper<String, Long>) Long::valueOf,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("queryMapValues").withValueSerde(Serdes.Long()))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        waitUntilAtLeastNumRecordProcessed(outputTopic, 5);

        final ReadOnlyKeyValueStore<String, Long> myMapStore =
            IntegrationTestUtils.getStore("queryMapValues", kafkaStreams, keyValueStore());

        for (final KeyValue<String, String> batchEntry : batch1) {
            assertEquals(Long.valueOf(batchEntry.value), myMapStore.get(batchEntry.key));
        }

        try (final KeyValueIterator<String, Long> range = myMapStore.range("hello", "kafka")) {
            while (range.hasNext()) {
                System.out.println(range.next());
            }
        }
    }

    @Test
    public void shouldBeAbleToQueryKeysWithGivenPrefix() throws Exception {
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        final Set<KeyValue<String, String>> batch1 = new HashSet<>(
            Arrays.asList(
                new KeyValue<>(keys[0], "1"),
                new KeyValue<>(keys[1], "1"),
                new KeyValue<>(keys[2], "3"),
                new KeyValue<>(keys[3], "5"),
                new KeyValue<>(keys[4], "2"))
        );

        final List<KeyValue<String, Long>> expectedPrefixScanResult = Arrays.asList(
            new KeyValue<>(keys[3], 5L),
            new KeyValue<>(keys[1], 1L)
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);

        final KTable<String, String> t1 = builder.table(streamOne);
        t1
            .mapValues(
                (ValueMapper<String, Long>) Long::valueOf,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("queryMapValues").withValueSerde(Serdes.Long()))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        waitUntilAtLeastNumRecordProcessed(outputTopic, 5);

        final ReadOnlyKeyValueStore<String, Long> myMapStore =
            IntegrationTestUtils.getStore("queryMapValues", kafkaStreams, keyValueStore());

        int index = 0;
        try (final KeyValueIterator<String, Long> range = myMapStore.prefixScan("go", new StringSerializer())) {
            while (range.hasNext()) {
                assertEquals(expectedPrefixScanResult.get(index++), range.next());
            }
        }
    }

    @Test
    public void shouldBeAbleToQueryMapValuesAfterFilterState() throws Exception {
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        final Set<KeyValue<String, String>> batch1 = new HashSet<>(
            Arrays.asList(
                new KeyValue<>(keys[0], "1"),
                new KeyValue<>(keys[1], "1"),
                new KeyValue<>(keys[2], "3"),
                new KeyValue<>(keys[3], "5"),
                new KeyValue<>(keys[4], "2"))
        );
        final Set<KeyValue<String, Long>> expectedBatch1 =
            Set.of(new KeyValue<>(keys[4], 2L));

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);

        final Predicate<String, String> filterPredicate = (key, value) -> key.contains("kafka");
        final KTable<String, String> t1 = builder.table(streamOne);
        final KTable<String, String> t2 = t1.filter(filterPredicate, Materialized.as("queryFilter"));
        final KTable<String, Long> t3 = t2
            .mapValues(
                (ValueMapper<String, Long>) Long::valueOf,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("queryMapValues").withValueSerde(Serdes.Long()));
        t3.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        waitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        final ReadOnlyKeyValueStore<String, Long> myMapStore =
            IntegrationTestUtils.getStore("queryMapValues", kafkaStreams, keyValueStore());

        for (final KeyValue<String, Long> expectedEntry : expectedBatch1) {
            assertEquals(expectedEntry.value, myMapStore.get(expectedEntry.key));
        }
        for (final KeyValue<String, String> batchEntry : batch1) {
            final KeyValue<String, Long> batchEntryMapValue =
                new KeyValue<>(batchEntry.key, Long.valueOf(batchEntry.value));
            if (!expectedBatch1.contains(batchEntryMapValue)) {
                assertNull(myMapStore.get(batchEntry.key));
            }
        }
    }

    private void verifyCanQueryState(final int cacheSizeBytes) throws Exception {
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, cacheSizeBytes);
        final StreamsBuilder builder = new StreamsBuilder();
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};

        final Set<KeyValue<String, String>> batch1 = new TreeSet<>(stringComparator);
        batch1.addAll(Arrays.asList(
            new KeyValue<>(keys[0], "hello"),
            new KeyValue<>(keys[1], "goodbye"),
            new KeyValue<>(keys[2], "welcome"),
            new KeyValue<>(keys[3], "go"),
            new KeyValue<>(keys[4], "kafka")));

        final Set<KeyValue<String, Long>> expectedCount = new TreeSet<>(stringLongComparator);
        for (final String key : keys) {
            expectedCount.add(new KeyValue<>(key, 1L));
        }

        IntegrationTestUtils.produceKeyValuesSynchronously(
                streamOne,
                batch1,
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()
                ),
                mockTime);

        final KStream<String, String> s1 = builder.stream(streamOne);

        // Non Windowed
        final String storeName = "my-count";
        s1.groupByKey()
            .count(Materialized.as(storeName))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final String windowStoreName = "windowed-count";
        s1.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(WINDOW_SIZE)))
            .count(Materialized.as(windowStoreName));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        waitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        final ReadOnlyKeyValueStore<String, Long> myCount =
            IntegrationTestUtils.getStore(storeName, kafkaStreams, keyValueStore());

        final ReadOnlyWindowStore<String, Long> windowStore =
            IntegrationTestUtils.getStore(windowStoreName, kafkaStreams, QueryableStoreTypes.windowStore());

        verifyCanGetByKey(keys,
            expectedCount,
            expectedCount,
            windowStore,
            myCount);

        verifyRangeAndAll(expectedCount, myCount);
    }

    @Test
    public void shouldNotMakeStoreAvailableUntilAllStoresAvailable() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(streamThree);

        final String storeName = "count-by-key";
        stream.groupByKey().count(Materialized.as(storeName));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        final KeyValue<String, String> hello = KeyValue.pair("hello", "hello");
        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamThree,
            Arrays.asList(hello, hello, hello, hello, hello, hello, hello, hello),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);

        final int maxWaitMs = 30000;

        final ReadOnlyKeyValueStore<String, Long> store =
            IntegrationTestUtils.getStore(storeName, kafkaStreams, keyValueStore());

        TestUtils.waitForCondition(
            () -> Long.valueOf(8).equals(store.get("hello")),
            maxWaitMs,
            "wait for count to be 8");

        // close stream
        kafkaStreams.close();

        // start again, and since it may take time to restore we wait for it to transit to RUNNING a bit longer
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        // make sure we never get any value other than 8 for hello
        TestUtils.waitForCondition(
            () -> {
                try {
                    assertEquals(8L, IntegrationTestUtils.getStore(storeName, kafkaStreams, keyValueStore()).get("hello"));
                    return true;
                } catch (final InvalidStateStoreException ise) {
                    return false;
                }
            },
            maxWaitMs,
            "waiting for store " + storeName);

    }


    @Test
    public void shouldAllowToQueryAfterThreadDied() throws Exception {
        final AtomicBoolean beforeFailure = new AtomicBoolean(true);
        final AtomicBoolean failed = new AtomicBoolean(false);
        final String storeName = "store";

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(streamOne);
        input
            .groupByKey()
            .reduce((value1, value2) -> {
                if (value1.length() > 1) {
                    if (beforeFailure.compareAndSet(true, false)) {
                        throw new RuntimeException("Injected test exception");
                    }
                }
                return value1 + value2;
            }, Materialized.as(storeName))
            .toStream()
            .to(outputTopic);

        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            failed.set(true);
            return REPLACE_THREAD;
        });

        startApplicationAndWaitUntilRunning(kafkaStreams);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            Arrays.asList(
                KeyValue.pair("a", "1"),
                KeyValue.pair("a", "2"),
                KeyValue.pair("b", "3"),
                KeyValue.pair("b", "4")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);

        final int maxWaitMs = 30000;

        final ReadOnlyKeyValueStore<String, String> store =
            IntegrationTestUtils.getStore(storeName, kafkaStreams, keyValueStore());

        TestUtils.waitForCondition(
            () -> "12".equals(store.get("a")) && "34".equals(store.get("b")),
            maxWaitMs,
            "wait for agg to be <a,12> and <b,34>");

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            Collections.singleton(KeyValue.pair("a", "5")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);

        TestUtils.waitForCondition(
            failed::get,
            maxWaitMs,
            "wait for thread to fail");

        final ReadOnlyKeyValueStore<String, String> store2 =
            IntegrationTestUtils.getStore(storeName, kafkaStreams, keyValueStore());

        try {
            TestUtils.waitForCondition(
                () -> ("125".equals(store2.get("a"))
                    || "1225".equals(store2.get("a"))
                    || "12125".equals(store2.get("a")))
                    &&
                    ("34".equals(store2.get("b"))
                    || "344".equals(store2.get("b"))
                    || "3434".equals(store2.get("b"))),
                maxWaitMs,
                "wait for agg to be <a,125>||<a,1225>||<a,12125> and <b,34>||<b,344>||<b,3434>");
        } catch (final Throwable t) {
            throw new RuntimeException("Store content is a: " + store2.get("a") + "; b: " + store2.get("b"), t);
        }
    }

    private void verifyRangeAndAll(final Set<KeyValue<String, Long>> expectedCount,
                                   final ReadOnlyKeyValueStore<String, Long> myCount) {
        final Set<KeyValue<String, Long>> countRangeResults = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>> countAllResults = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>> expectedRangeResults = new TreeSet<>(stringLongComparator);

        expectedRangeResults.addAll(Arrays.asList(
            new KeyValue<>("hello", 1L),
            new KeyValue<>("go", 1L),
            new KeyValue<>("goodbye", 1L),
            new KeyValue<>("kafka", 1L)
        ));

        try (final KeyValueIterator<String, Long> range = myCount.range("go", "kafka")) {
            while (range.hasNext()) {
                countRangeResults.add(range.next());
            }
        }

        try (final KeyValueIterator<String, Long> all = myCount.all()) {
            while (all.hasNext()) {
                countAllResults.add(all.next());
            }
        }

        assertEquals(expectedRangeResults, countRangeResults);
        assertEquals(expectedCount, countAllResults);
    }

    private void verifyCanGetByKey(final String[] keys,
                                   final Set<KeyValue<String, Long>> expectedWindowState,
                                   final Set<KeyValue<String, Long>> expectedCount,
                                   final ReadOnlyWindowStore<String, Long> windowStore,
                                   final ReadOnlyKeyValueStore<String, Long> myCount) throws Exception {
        final Set<KeyValue<String, Long>> windowState = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>> countState = new TreeSet<>(stringLongComparator);

        final long timeout = System.currentTimeMillis() + 30000;
        while ((windowState.size() < keys.length ||
            countState.size() < keys.length) &&
            System.currentTimeMillis() < timeout) {
            Thread.sleep(10);
            for (final String key : keys) {
                windowState.addAll(fetch(windowStore, key));
                final Long value = myCount.get(key);
                if (value != null) {
                    countState.add(new KeyValue<>(key, value));
                }
            }
        }
        assertEquals(expectedWindowState, windowState);
        assertEquals(expectedCount, countState);
    }

    private void waitUntilAtLeastNumRecordProcessed(final String topic,
                                                    final int numRecs) throws Exception {
        final Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
            config,
            topic,
            numRecs,
            DEFAULT_TIMEOUT_MS
        );
    }

    private Set<KeyValue<String, Long>> fetch(final ReadOnlyWindowStore<String, Long> store,
                                              final String key) {
        try (final WindowStoreIterator<Long> fetch =
                 store.fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()))) {
            if (fetch.hasNext()) {
                final KeyValue<Long, Long> next = fetch.next();
                return Collections.singleton(KeyValue.pair(key, next.value));
            }
        }

        return Collections.emptySet();
    }

    /**
     * Asserts that {@code actual} holds the entries of {@code expected}, in any order.
     */
    private static <T> void assertSameEntries(final Collection<T> expected, final List<T> actual) {
        assertEquals(expected.size(), actual.size());
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    private static <T> List<T> reversed(final List<T> list) {
        final List<T> reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
    }

    private static void produceSynchronously(final List<ProducerRecord<String, String>> records) throws Exception {
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class))) {
            for (final ProducerRecord<String, String> record : records) {
                producer.send(record).get();
            }
        }
    }

    /**
     * A class that periodically produces records in a separate thread
     */
    private static class ProducerRunnable implements Runnable {
        private final String topic;
        private final List<String> inputValues;
        private final int numIterations;
        private int currIteration = 0;

        ProducerRunnable(final String topic,
                         final List<String> inputValues,
                         final int numIterations) {
            this.topic = topic;
            this.inputValues = inputValues;
            this.numIterations = numIterations;
        }

        private synchronized void incrementIteration() {
            currIteration++;
        }

        synchronized int getCurrIteration() {
            return currIteration;
        }

        @Override
        public void run() {
            final Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (final KafkaProducer<String, String> producer =
                     new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer())) {

                while (getCurrIteration() < numIterations) {
                    for (final String value : inputValues) {
                        producer.send(new ProducerRecord<>(topic, value));
                    }
                    incrementIteration();
                }
            }
        }
    }

    @Test
    public void shouldQueryHeadersAwareKeyValueStoreViaMaterialized(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            // approximateNumEntries() only counts records that were flushed from the cache on commit
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100")
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            final List<KeyValue<String, String>> records = Arrays.asList(
                new KeyValue<>("alpha", "a1"),
                new KeyValue<>("beta", "b1"),
                new KeyValue<>("gamma", "g1"),
                new KeyValue<>("delta", "d1"),
                new KeyValue<>("epsilon", "e1")
            );
            final long timestamp = System.currentTimeMillis();
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                records,
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                timestamp,
                false
            );

            // sorted by key: "alpha", "beta", "delta", "epsilon", "gamma"
            final List<KeyValue<String, ValueTimestampHeaders<String>>> entries = records.stream()
                .sorted(Comparator.comparing(record -> record.key))
                .map(record -> KeyValue.pair(record.key, ValueTimestampHeaders.make(record.value, timestamp, new RecordHeaders())))
                .collect(Collectors.toList());

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNotNull(store);

                assertEquals(entries.get(0).value, store.get("alpha"));
                assertNull(store.get("nonexistent"));
                assertThrows(NullPointerException.class, () -> store.get(null));
                assertEquals(5L, store.approximateNumEntries());

                assertEquals(entries, toListAndCloseIterator(store.all()));
                assertEquals(reversed(entries), toListAndCloseIterator(store.reverseAll()));
                assertEquals(entries.subList(1, 4), toListAndCloseIterator(store.range("beta", "epsilon")));
                assertEquals(reversed(entries.subList(1, 4)), toListAndCloseIterator(store.reverseRange("beta", "epsilon")));
                assertEquals(entries.subList(2, 3), toListAndCloseIterator(store.prefixScan("de", new StringSerializer())));

                final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> tsStore =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStore()));
                assertNotNull(tsStore);
                final ValueAndTimestamp<String> tsResult = tsStore.get("beta");
                assertNotNull(tsResult);
                assertEquals("b1", tsResult.value());
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareKeyValueStoreViaDslConfig(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS)
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                Collections.singletonList(new KeyValue<>("key1", "val1")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                CLUSTER.time
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNotNull(store);
                final ValueTimestampHeaders<String> result = store.get("key1");
                assertNotNull(result);
                assertEquals("val1", result.value());
                assertEquals(new RecordHeaders(), result.headers());

                final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> tsStore =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStore()));
                assertNotNull(tsStore);
                final ValueAndTimestamp<String> tsResult = tsStore.get("key1");
                assertNotNull(tsResult);
                assertEquals("val1", tsResult.value());
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareWindowStoreOperations(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-window-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(WINDOW_SIZE)))
            .count(Materialized.<String, Long>as(
                Stores.persistentTimestampedWindowStoreWithHeaders(storeName, ofMillis(WINDOW_SIZE), ofMillis(WINDOW_SIZE), false))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            // a single timestamp puts all records of a key into the same window
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                Arrays.asList(
                    new KeyValue<>("w-key1", "v1"),
                    new KeyValue<>("w-key1", "v2"),
                    new KeyValue<>("w-key2", "v3")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                System.currentTimeMillis(),
                false
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyWindowStore<String, ValueTimestampHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedWindowStoreWithHeaders()));
                assertNotNull(store);

                final List<KeyValue<Long, ValueTimestampHeaders<Long>>> fetched = HeadersStores.toListWithEmptyHeaders(
                    store.fetch("w-key1", ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis() + WINDOW_SIZE)), ValueTimestampHeaders::headers);
                assertEquals(1, fetched.size());
                assertEquals(2L, fetched.get(0).value.value());

                assertEquals(2, HeadersStores.toListWithEmptyHeaders(store.all(), ValueTimestampHeaders::headers).size());

                final ReadOnlyWindowStore<String, ValueAndTimestamp<Long>> tsStore =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedWindowStore()));
                assertNotNull(tsStore);
                try (final WindowStoreIterator<ValueAndTimestamp<Long>> tsIter =
                         tsStore.fetch("w-key2", ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis() + WINDOW_SIZE))) {
                    assertTrue(tsIter.hasNext());
                    assertEquals(1L, tsIter.next().value.value());
                }
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareSessionStoreOperations(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-session-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(WINDOW_SIZE)))
            .count(Materialized.<String, Long>as(
                Stores.persistentSessionStoreWithHeaders(storeName, ofMillis(WINDOW_SIZE)))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                Arrays.asList(
                    new KeyValue<>("s-key1", "v1"),
                    new KeyValue<>("s-key1", "v2"),
                    new KeyValue<>("s-key2", "v3")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                CLUSTER.time
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlySessionStore<String, AggregationWithHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.sessionStoreWithHeaders()));
                assertNotNull(store);

                final List<KeyValue<Windowed<String>, AggregationWithHeaders<Long>>> sessions =
                    HeadersStores.toListWithEmptyHeaders(store.fetch("s-key1"), AggregationWithHeaders::headers);
                assertEquals(1, sessions.size());
                assertEquals(2L, sessions.get(0).value.aggregation());

                final ReadOnlySessionStore<String, Long> plainStore =
                    streams.store(fromNameAndType(storeName, sessionStore()));
                assertNotNull(plainStore);
                try (final KeyValueIterator<Windowed<String>, Long> plainIter = plainStore.fetch("s-key2")) {
                    assertTrue(plainIter.hasNext());
                    assertEquals(1L, plainIter.next().value);
                }
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareKeyValueStoreAfterRestart(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";
        final String stateDir = TestUtils.tempDirectory("headers-restart-test").getPath();

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, stateDir)
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                Arrays.asList(
                    new KeyValue<>("r-key1", "val1"),
                    new KeyValue<>("r-key2", "val2")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                CLUSTER.time
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNotNull(store.get("r-key1"));
                assertEquals("val1", store.get("r-key1").value());
            });
            HeadersStores.closeAndLeaveGroup(streams);
        }

        final StreamsBuilder builder2 = new StreamsBuilder();
        builder2.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        // keep the state directory, so that the restarted instance reopens the local store
        try (final KafkaStreams streams2 = getRunningStreams(properties, builder2, false)) {
            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams2.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNotNull(store);
                final ValueTimestampHeaders<String> result = store.get("r-key1");
                assertNotNull(result);
                assertEquals("val1", result.value());
                assertEquals(new RecordHeaders(), result.headers());

                final ValueTimestampHeaders<String> result2 = store.get("r-key2");
                assertNotNull(result2);
                assertEquals("val2", result2.value());
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareWindowStoreAcrossMultipleAndOverlappingWindows(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-window-store";
        final long windowSize = 10_000L;
        final long advance = 5_000L;

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            // hopping windows, so that every record is counted in two overlapping windows
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(windowSize)).advanceBy(ofMillis(advance)))
            .count(Materialized.<String, Long>as(
                Stores.persistentTimestampedWindowStoreWithHeaders(storeName, ofDays(1), ofMillis(windowSize), false))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            // aligned to the advance, so that a window starts at t
            final long t = System.currentTimeMillis() / advance * advance;
            produceSynchronously(Arrays.asList(
                new ProducerRecord<>(inputTopic, null, t, "wa", "v1", HeadersStores.dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t + advance, "wa", "v2", HeadersStores.dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t + 4 * advance, "wb", "v3", HeadersStores.dslInputHeaders())
            ));

            final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> windows = Arrays.asList(
                HeadersStores.windowedCount("wa", t - advance, t - advance + windowSize, 1L, t),
                HeadersStores.windowedCount("wa", t, t + windowSize, 2L, t + advance),
                HeadersStores.windowedCount("wa", t + advance, t + advance + windowSize, 1L, t + advance),
                HeadersStores.windowedCount("wb", t + 3 * advance, t + 3 * advance + windowSize, 1L, t + 4 * advance),
                HeadersStores.windowedCount("wb", t + 4 * advance, t + 4 * advance + windowSize, 1L, t + 4 * advance)
            );
            final List<KeyValue<Long, ValueTimestampHeaders<Long>>> waWindows = windows.subList(0, 3).stream()
                .map(window -> KeyValue.pair(window.key.window().start(), window.value))
                .collect(Collectors.toList());
            final Instant timeFrom = ofEpochMilli(t - advance);
            final Instant timeTo = ofEpochMilli(t + 4 * advance);

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyWindowStore<String, ValueTimestampHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedWindowStoreWithHeaders()));
                assertNotNull(store);

                assertEquals(waWindows, toListAndCloseIterator(store.fetch("wa", timeFrom, timeTo)));
                assertEquals(reversed(waWindows), toListAndCloseIterator(store.backwardFetch("wa", timeFrom, timeTo)));
                // the time range bounds the start time of the windows
                assertEquals(waWindows.subList(1, 2), toListAndCloseIterator(store.fetch("wa", ofEpochMilli(t), ofEpochMilli(t))));
                assertEquals(waWindows.get(1).value, store.fetch("wa", t));

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> fetchedAll =
                    toListAndCloseIterator(store.fetchAll(timeFrom, timeTo));
                assertSameEntries(windows, fetchedAll);
                assertEquals(reversed(fetchedAll), toListAndCloseIterator(store.backwardFetchAll(timeFrom, timeTo)));

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> all = toListAndCloseIterator(store.all());
                assertSameEntries(windows, all);
                assertEquals(reversed(all), toListAndCloseIterator(store.backwardAll()));

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> keyRange =
                    toListAndCloseIterator(store.fetch("wa", "wb", timeFrom, timeTo));
                assertSameEntries(windows, keyRange);
                assertEquals(reversed(keyRange), toListAndCloseIterator(store.backwardFetch("wa", "wb", timeFrom, timeTo)));
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareSessionStoreWithMultipleAndMergedSessions(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-session-store";
        final long inactivityGap = 5_000L;

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapAndGrace(ofMillis(inactivityGap), ofMinutes(1)))
            .count(Materialized.<String, Long>as(
                Stores.persistentSessionStoreWithHeaders(storeName, ofDays(1)))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            final long t = System.currentTimeMillis();
            // the records of "sa" are further apart than the inactivity gap, so they are in two sessions
            produceSynchronously(Arrays.asList(
                new ProducerRecord<>(inputTopic, null, t, "sa", "v1", HeadersStores.dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t, "sb", "v2", HeadersStores.dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t + 2 * inactivityGap, "sa", "v3", HeadersStores.dslInputHeaders())
            ));

            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> firstSession = HeadersStores.sessionCount("sa", t, t, 1L);
            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> secondSession =
                HeadersStores.sessionCount("sa", t + 2 * inactivityGap, t + 2 * inactivityGap, 1L);
            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> otherKeySession = HeadersStores.sessionCount("sb", t, t, 1L);

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlySessionStore<String, AggregationWithHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.sessionStoreWithHeaders()));
                assertNotNull(store);

                assertEquals(Arrays.asList(firstSession, secondSession), toListAndCloseIterator(store.fetch("sa")));
                assertEquals(Arrays.asList(secondSession, firstSession), toListAndCloseIterator(store.backwardFetch("sa")));
                // only the second session ends after t
                assertEquals(
                    Collections.singletonList(secondSession),
                    toListAndCloseIterator(store.findSessions("sa", t + 1, t + 2 * inactivityGap)));
                // only the first session starts at or before t
                assertEquals(
                    Collections.singletonList(firstSession),
                    toListAndCloseIterator(store.backwardFindSessions("sa", t, t)));

                final List<KeyValue<Windowed<String>, AggregationWithHeaders<Long>>> keyRange =
                    toListAndCloseIterator(store.fetch("sa", "sb"));
                assertSameEntries(Arrays.asList(firstSession, secondSession, otherKeySession), keyRange);
                assertEquals(reversed(keyRange), toListAndCloseIterator(store.backwardFetch("sa", "sb")));
            });

            // a record within the inactivity gap of both sessions merges them into one
            produceSynchronously(Collections.singletonList(
                new ProducerRecord<>(inputTopic, null, t + inactivityGap, "sa", "v4", HeadersStores.dslInputHeaders())));

            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> mergedSession =
                HeadersStores.sessionCount("sa", t, t + 2 * inactivityGap, 3L);

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlySessionStore<String, AggregationWithHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.sessionStoreWithHeaders()));

                assertEquals(Collections.singletonList(mergedSession), toListAndCloseIterator(store.fetch("sa")));
                assertEquals(Collections.singletonList(mergedSession), toListAndCloseIterator(store.findSessions("sa", t, t)));
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareKeyValueStoreAfterUpdateAndTombstone(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                Collections.singletonList(new KeyValue<>("key", "v1")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                CLUSTER.time
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                final ValueTimestampHeaders<String> result = store.get("key");
                assertNotNull(result);
                assertEquals("v1", result.value());
            });

            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                Collections.singletonList(new KeyValue<>("key", "v2")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                CLUSTER.time
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                final ValueTimestampHeaders<String> result = store.get("key");
                assertNotNull(result);
                assertEquals("v2", result.value());
                assertEquals(new RecordHeaders(), result.headers());
                assertEquals(1, toListAndCloseIterator(store.all()).size());
            });

            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                Collections.singletonList(new KeyValue<>("key", (String) null)),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                HeadersStores.dslInputHeaders(),
                CLUSTER.time
            );

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNull(store.get("key"));
                assertEquals(Collections.emptyList(), toListAndCloseIterator(store.all()));
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldReturnRecordHeadersFromHeadersAwareStores(final boolean cachingEnabled, final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic);

        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, HeadersStores.topology(inputTopic, cachingEnabled), true)) {
            final List<ProducerRecord<String, String>> records = HeadersStores.recordsWithHeaders(inputTopic);
            produceSynchronously(records);

            // sorted by key: "duplicate", "multi", "none", "single"
            final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = HeadersStores.latestValuesByKey(records);
            final ProducerRecord<String, String> firstRecord = records.get(0);
            final ProducerRecord<String, String> lastRecord = records.get(records.size() - 1);
            final Instant timeFrom = ofEpochMilli(HeadersStores.windowStart(firstRecord.timestamp()));
            final Instant timeTo = ofEpochMilli(HeadersStores.windowStart(lastRecord.timestamp()));

            retryOnExceptionWithTimeout(30_000, () -> {
                for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
                    HeadersStores.verifyForKey(streams, null, false, latestValue.key, records);
                }

                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> kvStore = HeadersStores.store(
                    streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false);
                assertEquals(latestValues, toListAndCloseIterator(kvStore.all()));
                assertEquals(reversed(latestValues), toListAndCloseIterator(kvStore.reverseAll()));
                assertEquals(latestValues.subList(1, 3), toListAndCloseIterator(kvStore.range("multi", "none")));
                assertEquals(reversed(latestValues.subList(1, 3)), toListAndCloseIterator(kvStore.reverseRange("multi", "none")));
                assertEquals(latestValues.subList(3, 4), toListAndCloseIterator(kvStore.prefixScan("si", new StringSerializer())));
                assertThrows(NullPointerException.class, () -> kvStore.get(null));

                final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> windowStore = HeadersStores.store(
                    streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.timestampedWindowStoreWithHeaders(), null, false);
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> windows =
                    records.stream().map(HeadersStores::window).collect(Collectors.toList());
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> allWindows =
                    toListAndCloseIterator(windowStore.all());
                assertSameEntries(windows, allWindows);
                assertEquals(reversed(allWindows), toListAndCloseIterator(windowStore.backwardAll()));
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> fetchedWindows =
                    toListAndCloseIterator(windowStore.fetchAll(timeFrom, timeTo));
                assertSameEntries(windows, fetchedWindows);
                assertEquals(reversed(fetchedWindows), toListAndCloseIterator(windowStore.backwardFetchAll(timeFrom, timeTo)));
                // only the last record is in the later window
                assertEquals(
                    Collections.singletonList(HeadersStores.window(lastRecord)),
                    toListAndCloseIterator(windowStore.fetch("duplicate", "single", timeTo, timeTo)));
                assertEquals(HeadersStores.valueTimestampHeaders(firstRecord), windowStore.fetch("multi", timeFrom.toEpochMilli()));
                assertThrows(NullPointerException.class, () -> windowStore.fetch(null, timeFrom, timeTo));

                final ReadOnlySessionStore<String, AggregationWithHeaders<String>> sessionStore = HeadersStores.store(
                    streams, HEADERS_SESSION_STORE, QueryableStoreTypes.sessionStoreWithHeaders(), null, false);
                final List<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> fetchedSessions =
                    toListAndCloseIterator(sessionStore.fetch("duplicate", "single"));
                assertSameEntries(
                    records.stream().map(HeadersStores::session).collect(Collectors.toList()),
                    fetchedSessions);
                assertEquals(reversed(fetchedSessions), toListAndCloseIterator(sessionStore.backwardFetch("duplicate", "single")));
                // only the session of the last record ends after the first record
                assertEquals(
                    Collections.singletonList(HeadersStores.session(lastRecord)),
                    toListAndCloseIterator(sessionStore.findSessions("multi", firstRecord.timestamp() + 1, lastRecord.timestamp())));
                assertEquals(
                    Collections.singletonList(HeadersStores.session(firstRecord)),
                    toListAndCloseIterator(sessionStore.backwardFindSessions("multi", firstRecord.timestamp(), firstRecord.timestamp())));
                assertThrows(NullPointerException.class, () -> sessionStore.fetch(null));
            });
        }
    }

    @Test
    public void shouldReturnRecordHeadersFromActiveAndStandbyHostsBeforeAndAfterRebalance(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic, STREAM_TWO_PARTITIONS, NUM_REPLICAS);

        final List<ProducerRecord<String, String>> records = HeadersStores.recordsWithHeaders(inputTopic);
        final Set<String> keys = records.stream().map(ProducerRecord::key).collect(Collectors.toSet());
        produceSynchronously(records);

        final List<KafkaStreams> streamsList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final Properties properties = mkProperties(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                // the port is the index of the instance in streamsList
                mkEntry(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + i),
                mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1"),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
            ));
            streamsList.add(new KafkaStreams(HeadersStores.topology(inputTopic, false).build(), properties));
        }

        try {
            startApplicationAndWaitUntilRunning(streamsList, ofSeconds(60));

            final StringSerializer serializer = new StringSerializer();
            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> {
                for (final String key : keys) {
                    final KeyQueryMetadata metadata = streamsList.get(0).queryMetadataForKey(HEADERS_KV_STORE, key, serializer);
                    assertEquals(1, metadata.standbyHosts().size());
                    final KafkaStreams activeHost = streamsList.get(metadata.activeHost().port());
                    final KafkaStreams standbyHost = streamsList.get(metadata.standbyHosts().iterator().next().port());

                    HeadersStores.verifyForKey(activeHost, metadata.partition(), false, key, records);
                    // the stores of standby tasks are only queryable as stale stores
                    HeadersStores.verifyForKey(standbyHost, metadata.partition(), true, key, records);
                }
            });

            // the remaining instance takes over the active tasks of the closed one
            HeadersStores.closeAndLeaveGroup(streamsList.get(1));
            waitForApplicationState(streamsList.subList(0, 1), State.RUNNING, ofSeconds(60));

            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> {
                for (final String key : keys) {
                    HeadersStores.verifyForKey(streamsList.get(0), null, false, key, records);
                }
            });
        } finally {
            for (final KafkaStreams streams : streamsList) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldReturnRecordHeadersFromHeadersAwareStoresDuringAndAfterRestoration(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic);

        final List<ProducerRecord<String, String>> records = HeadersStores.recordsWithHeaders(inputTopic);
        final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = HeadersStores.latestValuesByKey(records);
        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));

        try (final KafkaStreams streams = getRunningStreams(properties, HeadersStores.topology(inputTopic, false), true)) {
            produceSynchronously(records);
            retryOnExceptionWithTimeout(30_000, () -> assertEquals(latestValues, toListAndCloseIterator(HeadersStores.store(
                streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false).all())));
            HeadersStores.closeAndLeaveGroup(streams);
        }

        // with an empty state directory, the stores are restored from their changelogs
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        // restore one changelog record per batch, so that restoration can be paused after the first record
        properties.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);

        final PausingRestoreListener restoreListener = new PausingRestoreListener(HEADERS_KV_STORE);
        try (final KafkaStreams streams = new KafkaStreams(HeadersStores.topology(inputTopic, false).build(), properties)) {
            streams.setGlobalStateRestoreListener(restoreListener);
            streams.start();

            try {
                restoreListener.awaitPause();
                // the restoring store is not queryable as an active store
                assertThrows(InvalidStateStoreException.class, () -> HeadersStores.store(
                    streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false)
                    .get(records.get(0).key()));
                // only the first changelog record is restored, and it is queryable as a stale store
                assertEquals(
                    Collections.singletonList(KeyValue.pair(records.get(0).key(), HeadersStores.valueTimestampHeaders(records.get(0)))),
                    toListAndCloseIterator(HeadersStores.store(
                        streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, true).all()));
            } finally {
                restoreListener.resume();
            }

            waitForApplicationState(Collections.singletonList(streams), State.RUNNING, ofSeconds(60));
            retryOnExceptionWithTimeout(30_000, () -> {
                for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
                    HeadersStores.verifyForKey(streams, null, false, latestValue.key, records);
                }
            });
        }
    }

    /**
     * A state listener that records how many times each state was entered, so that tests can assert
     * a rebalance has happened.
     */
    private static class StateListenerStub implements KafkaStreams.StateListener {
        private final Map<KafkaStreams.State, Long> mapStates = new HashMap<>();

        @Override
        public void onChange(final KafkaStreams.State newState,
                             final KafkaStreams.State oldState) {
            mapStates.merge(newState, 1L, Long::sum);
        }
    }

    /**
     * A restore listener that pauses the restoration of a store after its first restored batch, until
     * {@link #resume()} is called.
     */
    private static class PausingRestoreListener implements StateRestoreListener {
        private final String pausedStoreName;
        private final CountDownLatch firstBatchRestored = new CountDownLatch(1);
        private final CountDownLatch resumeRestoration = new CountDownLatch(1);

        PausingRestoreListener(final String pausedStoreName) {
            this.pausedStoreName = pausedStoreName;
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            if (storeName.equals(pausedStoreName) && firstBatchRestored.getCount() > 0) {
                firstBatchRestored.countDown();
                try {
                    resumeRestoration.await(60, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
        }

        void awaitPause() throws InterruptedException {
            assertTrue(firstBatchRestored.await(60, TimeUnit.SECONDS), "the restoration of " + pausedStoreName + " was not paused");
        }

        void resume() {
            resumeRestoration.countDown();
        }
    }

    /**
     * Helpers for the tests of the headers-aware stores, including a Processor API topology that stores the
     * headers of its input records.
     */
    private static final class HeadersStores {
        /**
         * Drains and closes {@code iterator}, asserting that every entry carries empty headers,
         * since the DSL does not write record headers into headers-aware stores.
         */
        private static <K, V> List<KeyValue<K, V>> toListWithEmptyHeaders(final KeyValueIterator<K, V> iterator,
                                                                          final Function<V, Headers> headersExtractor) {
            final List<KeyValue<K, V>> entries = toListAndCloseIterator(iterator);
            for (final KeyValue<K, V> entry : entries) {
                assertEquals(new RecordHeaders(), headersExtractor.apply(entry.value));
            }
            return entries;
        }

        private static Headers headers(final String... keysAndValues) {
            final Headers headers = new RecordHeaders();
            for (int i = 0; i < keysAndValues.length; i += 2) {
                headers.add(keysAndValues[i], keysAndValues[i + 1].getBytes(StandardCharsets.UTF_8));
            }
            return headers;
        }

        /**
         * Headers of the input records of the DSL tests. DSL operators write empty headers to their stores
         * (see the KIP-1285 current limitations), so these headers must not show up in query results.
         */
        private static Headers dslInputHeaders() {
            return headers("source", "dsl-input");
        }

        /**
         * Closes {@code streams} and leaves the group, so that the next instance of the application does not
         * wait for the session timeout of this one to get its tasks.
         */
        private static void closeAndLeaveGroup(final KafkaStreams streams) {
            assertTrue(streams.close(
                CloseOptions.groupMembershipOperation(GroupMembershipOperation.LEAVE_GROUP).withTimeout(ofSeconds(60))));
        }

        private static KeyValue<Windowed<String>, ValueTimestampHeaders<Long>> windowedCount(final String key,
                                                                                           final long windowStart,
                                                                                           final long windowEnd,
                                                                                           final long count,
                                                                                           final long timestamp) {
            return KeyValue.pair(
                new Windowed<>(key, new TimeWindow(windowStart, windowEnd)),
                ValueTimestampHeaders.make(count, timestamp, new RecordHeaders()));
        }

        private static KeyValue<Windowed<String>, AggregationWithHeaders<Long>> sessionCount(final String key,
                                                                                          final long sessionStart,
                                                                                          final long sessionEnd,
                                                                                          final long count) {
            return KeyValue.pair(
                new Windowed<>(key, new SessionWindow(sessionStart, sessionEnd)),
                AggregationWithHeaders.make(count, new RecordHeaders()));
        }

        /**
         * Builds a Processor API topology that writes every input record, together with its headers, into a
         * headers-aware key-value, window, and session store. Unlike the DSL, which writes empty headers to its
         * stores, this lets tests verify that IQv1 returns the headers of the stored records.
         */
        private static StreamsBuilder topology(final String inputTopic, final boolean cachingEnabled) {
            final List<StoreBuilder<?>> storeBuilders = Arrays.asList(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(HEADERS_KV_STORE),
                    Serdes.String(),
                    Serdes.String()),
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(HEADERS_WINDOW_STORE, ofDays(1), ofMillis(HEADERS_WINDOW_SIZE_MS), false),
                    Serdes.String(),
                    Serdes.String()),
                Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStoreWithHeaders(HEADERS_SESSION_STORE, ofDays(1)),
                    Serdes.String(),
                    Serdes.String())
            );

            final StreamsBuilder builder = new StreamsBuilder();
            for (final StoreBuilder<?> storeBuilder : storeBuilders) {
                builder.addStateStore(cachingEnabled ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled());
            }
            builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .process(Writer::new, HEADERS_KV_STORE, HEADERS_WINDOW_STORE, HEADERS_SESSION_STORE);
            return builder;
        }

        /**
         * Records with multiple headers whose keys are not sorted, a repeated header key, a single header, and no
         * headers. Key "multi" is written twice, into two different windows and sessions.
         */
        private static List<ProducerRecord<String, String>> recordsWithHeaders(final String topic) {
            final long timestamp = System.currentTimeMillis();
            return Arrays.asList(
                new ProducerRecord<>(topic, null, timestamp, "multi", "v1", headers("z", "1", "a", "2", "m", "3")),
                new ProducerRecord<>(topic, null, timestamp, "duplicate", "v2", headers("dup", "1", "dup", "2")),
                new ProducerRecord<>(topic, null, timestamp, "single", "v3", headers("key", "value")),
                new ProducerRecord<>(topic, null, timestamp, "none", "v4", headers()),
                new ProducerRecord<>(topic, null, timestamp + HEADERS_WINDOW_SIZE_MS, "multi", "v5", headers("z", "4"))
            );
        }

        private static long windowStart(final long timestamp) {
            return timestamp - timestamp % HEADERS_WINDOW_SIZE_MS;
        }

        private static ValueTimestampHeaders<String> valueTimestampHeaders(final ProducerRecord<String, String> record) {
            return ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
        }

        private static KeyValue<Windowed<String>, ValueTimestampHeaders<String>> window(final ProducerRecord<String, String> record) {
            final long windowStart = windowStart(record.timestamp());
            return KeyValue.pair(
                new Windowed<>(record.key(), new TimeWindow(windowStart, windowStart + HEADERS_WINDOW_SIZE_MS)),
                valueTimestampHeaders(record));
        }

        private static KeyValue<Windowed<String>, AggregationWithHeaders<String>> session(final ProducerRecord<String, String> record) {
            return KeyValue.pair(
                new Windowed<>(record.key(), new SessionWindow(record.timestamp(), record.timestamp())),
                AggregationWithHeaders.make(record.value(), record.headers()));
        }

        /**
         * @return the value, timestamp, and headers of the latest record of each key, sorted by key
         */
        private static List<KeyValue<String, ValueTimestampHeaders<String>>> latestValuesByKey(final List<ProducerRecord<String, String>> records) {
            final Map<String, ValueTimestampHeaders<String>> latestValues = new TreeMap<>();
            for (final ProducerRecord<String, String> record : records) {
                latestValues.put(record.key(), valueTimestampHeaders(record));
            }
            return latestValues.entrySet().stream()
                .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }

        private static <T> T store(final KafkaStreams streams,
                                          final String storeName,
                                          final QueryableStoreType<T> queryableStoreType,
                                          final Integer partition,
                                          final boolean enableStaleStores) {
            final StoreQueryParameters<T> parameters = fromNameAndType(storeName, queryableStoreType).withPartition(partition);
            return streams.store(enableStaleStores ? parameters.enableStaleStores() : parameters);
        }

        /**
         * Verifies that single-key queries of the stores of {@link #topology} return the value,
         * timestamp, and headers of the records produced for {@code key}.
         */
        private static void verifyForKey(final KafkaStreams streams,
                                                      final Integer partition,
                                                      final boolean enableStaleStores,
                                                      final String key,
                                                      final List<ProducerRecord<String, String>> records) {
            final List<ProducerRecord<String, String>> keyRecords = records.stream()
                .filter(record -> record.key().equals(key))
                .collect(Collectors.toList());
            final ProducerRecord<String, String> latestRecord = keyRecords.get(keyRecords.size() - 1);

            final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> kvStore = store(
                streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), partition, enableStaleStores);
            assertEquals(valueTimestampHeaders(latestRecord), kvStore.get(key));

            final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> windowStore = store(
                streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.timestampedWindowStoreWithHeaders(), partition, enableStaleStores);
            assertEquals(
                keyRecords.stream()
                    .map(record -> KeyValue.pair(windowStart(record.timestamp()), valueTimestampHeaders(record)))
                    .collect(Collectors.toList()),
                toListAndCloseIterator(windowStore.fetch(key, ofEpochMilli(0L), ofEpochMilli(latestRecord.timestamp()))));

            final ReadOnlySessionStore<String, AggregationWithHeaders<String>> sessionStore = store(
                streams, HEADERS_SESSION_STORE, QueryableStoreTypes.sessionStoreWithHeaders(), partition, enableStaleStores);
            assertEquals(
                keyRecords.stream().map(HeadersStores::session).collect(Collectors.toList()),
                toListAndCloseIterator(sessionStore.fetch(key)));
        }

        /**
         * Writes every record, together with its headers, into the stores of {@link #topology}:
         * the window store puts it into the window that contains its timestamp, and the session store into
         * a session that starts and ends at its timestamp.
         */
        private static class Writer implements Processor<String, String, Void, Void> {
            private TimestampedKeyValueStoreWithHeaders<String, String> kvStore;
            private TimestampedWindowStoreWithHeaders<String, String> windowStore;
            private SessionStoreWithHeaders<String, String> sessionStore;

            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                kvStore = context.getStateStore(HEADERS_KV_STORE);
                windowStore = context.getStateStore(HEADERS_WINDOW_STORE);
                sessionStore = context.getStateStore(HEADERS_SESSION_STORE);
            }

            @Override
            public void process(final Record<String, String> record) {
                final long timestamp = record.timestamp();
                final ValueTimestampHeaders<String> valueTimestampHeaders =
                    ValueTimestampHeaders.make(record.value(), timestamp, record.headers());
                kvStore.put(record.key(), valueTimestampHeaders);
                windowStore.put(record.key(), valueTimestampHeaders, windowStart(timestamp));
                sessionStore.put(
                    new Windowed<>(record.key(), new SessionWindow(timestamp, timestamp)),
                    AggregationWithHeaders.make(record.value(), record.headers()));
            }
        }
    }
}
