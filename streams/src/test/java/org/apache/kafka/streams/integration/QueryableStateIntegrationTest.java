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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreamsTest;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.LagInfo;
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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getRunningStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.sessionStore;
import static org.apache.kafka.test.StreamsTestUtils.startKafkaStreamsAndWaitForRunningState;
import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@Category({IntegrationTest.class})
public class QueryableStateIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(QueryableStateIntegrationTest.class);

    private static final long DEFAULT_TIMEOUT_MS = 120 * 1000;

    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
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
    private static final int STREAM_TWO_PARTITIONS = 2;
    private static final int NUM_REPLICAS = NUM_BROKERS;
    private Properties streamsConfiguration;
    private List<String> inputValues;
    private Set<String> inputValuesKeys;
    private KafkaStreams kafkaStreams;
    private Comparator<KeyValue<String, String>> stringComparator;
    private Comparator<KeyValue<String, Long>> stringLongComparator;

    private void createTopics() throws Exception {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
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

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception {
        createTopics();
        streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        stringComparator = Comparator.comparing((KeyValue<String, String> o) -> o.key).thenComparing(o -> o.value);
        stringLongComparator = Comparator.comparing((KeyValue<String, Long> o) -> o.key).thenComparingLong(o -> o.value);
        inputValues = getInputValues();
        inputValuesKeys = new HashSet<>();
        for (final String sentence : inputValues) {
            final String[] words = sentence.split("\\W+");
            Collections.addAll(inputValuesKeys, words);
        }
    }

    @After
    public void shutdown() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close(ofSeconds(30));
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        CLUSTER.deleteAllTopicsAndWait(0L);
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
            .windowedBy(TimeWindows.of(ofMillis(WINDOW_SIZE)))
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
            assertThat(localLags.values().stream().mapToInt(Map::size).sum(), equalTo(expectedPartitions));
            if (expectedPartitions > 0) {
                assertThat(localLags.keySet(), equalTo(stores));
            }
        }
    }

    private void verifyAllKVKeys(final List<KafkaStreams> streamsList,
                                 final KafkaStreams streams,
                                 final KafkaStreamsTest.StateListenerStub stateListener,
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
                        assertThat("Should have standbys to query from", !queryMetadata.standbyHosts().isEmpty());
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
                                       final KafkaStreamsTest.StateListenerStub stateListenerStub,
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
                        assertThat(queryMetadata.standbyHosts().size(), equalTo(0));
                    } else {
                        assertThat("Should have standbys to query from", !queryMetadata.standbyHosts().isEmpty());
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

        assertThat(reason.toString(),
            noMetadataKeys.isEmpty() && nullStoreKeys.isEmpty() && nullValueKeys.isEmpty() && exceptionalKeys.isEmpty());
    }

    @Test
    public void shouldRejectNonExistentStoreName() throws InterruptedException {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
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
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, safeUniqueTestName(getClass(), testName)),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));

        CLUSTER.createTopic(input);

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            final ReadOnlyKeyValueStore<String, String> store =
                streams.store(fromNameAndType(storeName, keyValueStore()));
            assertThat(store, Matchers.notNullValue());

            final UnknownStateStoreException exception = assertThrows(
                UnknownStateStoreException.class,
                () -> streams.store(fromNameAndType("no-table", keyValueStore()))
            );
            assertThat(
                exception.getMessage(),
                is("Cannot get state store no-table because no such store is registered in the topology.")
            );
        }
    }

    @Test
    public void shouldRejectWronglyTypedStore() throws InterruptedException {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
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
            assertThat(store, Matchers.notNullValue());

            // Note that to check the type we actually need a store reference,
            // so we can't check when you get the IQ store, only when you
            // try to use it. Presumably, this could be improved.
            final ReadOnlySessionStore<String, String> sessionStore =
                streams.store(fromNameAndType(storeName, sessionStore()));
            final InvalidStateStoreException exception = assertThrows(
                InvalidStateStoreException.class,
                () -> sessionStore.fetch("a")
            );
            assertThat(
                exception.getMessage(),
                is(
                    "Cannot get state store " + storeName + " because the queryable store type" +
                        " [class org.apache.kafka.streams.state.QueryableStoreTypes$SessionStoreType]" +
                        " does not accept the actual store type" +
                        " [class org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore]."
                )
            );
        }
    }

    @Test
    public void shouldBeAbleToQueryDuringRebalance() throws Exception {
        final int numThreads = STREAM_TWO_PARTITIONS;
        final List<KafkaStreams> streamsList = new ArrayList<>(numThreads);
        final List<KafkaStreamsTest.StateListenerStub> listeners = new ArrayList<>(numThreads);

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
            final KafkaStreamsTest.StateListenerStub listener = new KafkaStreamsTest.StateListenerStub();
            streams.setStateListener(listener);
            listeners.add(listener);
            streamsList.add(streams);
        }
        startApplicationAndWaitUntilRunning(streamsList, Duration.ofSeconds(60));

        final Set<String> stores = mkSet(storeName + "-" + streamThree, windowStoreName + "-" + streamThree);
        verifyOffsetLagFetch(streamsList, stores, Arrays.asList(4, 4));

        try {
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
                assertThat(String.format("Streams instance %s did not close in %d ms", i, closeTimeout.toMillis()),
                    streamsList.get(i).close(closeTimeout));
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
        final List<KafkaStreamsTest.StateListenerStub> listeners = new ArrayList<>(numThreads);

        final ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, 1);
        producerRunnable.run();

        // create stream threads
        final String storeName = "word-count-store";
        final String windowStoreName = "windowed-word-count-store";
        final Set<String> stores = mkSet(storeName + "-" + streamThree, windowStoreName + "-" + streamThree);
        for (int i = 0; i < numThreads; i++) {
            final Properties props = (Properties) streamsConfiguration.clone();
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + i);
            props.put(StreamsConfig.CLIENT_ID_CONFIG, "instance-" + i);
            props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("shouldBeAbleQueryStandbyStateDuringRebalance-" + i).getPath());
            final KafkaStreams streams =
                createCountStream(streamThree, outputTopicThree, outputTopicConcurrentWindowed, storeName, windowStoreName, props);
            final KafkaStreamsTest.StateListenerStub listener = new KafkaStreamsTest.StateListenerStub();
            streams.setStateListener(listener);
            listeners.add(listener);
            streamsList.add(streams);
        }
        startApplicationAndWaitUntilRunning(streamsList, ofSeconds(60));
        verifyOffsetLagFetch(streamsList, stores, Arrays.asList(8, 8));

        try {
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
                assertThat(String.format("Streams instance %s did not close in %d ms", i, closeTimeout.toMillis()),
                    streamsList.get(i).close(closeTimeout));
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
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
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
            new HashSet<>(Collections.singleton(new KeyValue<>(keys[4], 2L)));

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
        startKafkaStreamsAndWaitForRunningState(kafkaStreams);

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
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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
        startKafkaStreamsAndWaitForRunningState(kafkaStreams);

        waitUntilAtLeastNumRecordProcessed(outputTopic, 5);

        final ReadOnlyKeyValueStore<String, Long> myMapStore =
            IntegrationTestUtils.getStore("queryMapValues", kafkaStreams, keyValueStore());

        for (final KeyValue<String, String> batchEntry : batch1) {
            assertEquals(Long.valueOf(batchEntry.value), myMapStore.get(batchEntry.key));
        }
    }

    @Test
    public void shouldBeAbleToQueryMapValuesAfterFilterState() throws Exception {
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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
            new HashSet<>(Collections.singleton(new KeyValue<>(keys[4], 2L)));

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
        startKafkaStreamsAndWaitForRunningState(kafkaStreams);

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
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);
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
                new Properties()),
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
            .windowedBy(TimeWindows.of(ofMillis(WINDOW_SIZE)))
            .count(Materialized.as(windowStoreName));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        startKafkaStreamsAndWaitForRunningState(kafkaStreams);

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
        startKafkaStreamsAndWaitForRunningState(kafkaStreams);

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
        startKafkaStreamsAndWaitForRunningState(kafkaStreams, maxWaitMs);

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
    @Deprecated //A single thread should no longer die
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
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> failed.set(true));

        // since we start with two threads, wait for a bit longer for both of them to transit to running
        startKafkaStreamsAndWaitForRunningState(kafkaStreams, 30000);

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

        assertThat(countRangeResults, equalTo(expectedRangeResults));
        assertThat(countAllResults, equalTo(expectedCount));
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
        assertThat(windowState, equalTo(expectedWindowState));
        assertThat(countState, equalTo(expectedCount));
    }

    private void waitUntilAtLeastNumRecordProcessed(final String topic,
                                                    final int numRecs) throws Exception {
        final long timeout = DEFAULT_TIMEOUT_MS;
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
            timeout);
    }

    private Set<KeyValue<String, Long>> fetch(final ReadOnlyWindowStore<String, Long> store,
                                              final String key) {
        final WindowStoreIterator<Long> fetch =
            store.fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
        if (fetch.hasNext()) {
            final KeyValue<Long, Long> next = fetch.next();
            return Collections.singleton(KeyValue.pair(key, next.value));
        }
        return Collections.emptySet();
    }

    /**
     * A class that periodically produces records in a separate thread
     */
    private class ProducerRunnable implements Runnable {
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

}
