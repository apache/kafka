/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(Parameterized.class)
public class QueryableStateIntegrationTest {
    private static final int NUM_BROKERS = 1;
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;
    private String streamOne = "stream-one";
    private String streamTwo = "stream-two";
    private String streamThree = "stream-three";
    private String streamConcurrent = "stream-concurrent";
    private String outputTopic = "output";
    private String outputTopicConcurrent = "output-concurrent";
    private String outputTopicThree = "output-three";
    // sufficiently large window size such that everything falls into 1 window
    private static final long WINDOW_SIZE = TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS);
    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_REPLICAS = NUM_BROKERS;
    private Properties streamsConfiguration;
    private List<String> inputValues;
    private Set<String> inputValuesKeys;
    private KafkaStreams kafkaStreams;
    private Comparator<KeyValue<String, String>> stringComparator;
    private Comparator<KeyValue<String, Long>> stringLongComparator;
    private static int testNo = 0;

    public void createTopics() {
        streamOne = streamOne + "-" + testNo;
        streamConcurrent = streamConcurrent + "-" + testNo;
        streamThree = streamThree + "-" + testNo;
        outputTopic = outputTopic + "-" + testNo;
        outputTopicConcurrent = outputTopicConcurrent + "-" + testNo;
        outputTopicThree = outputTopicThree + "-" + testNo;
        streamTwo = streamTwo + "-" + testNo;
        CLUSTER.createTopic(streamOne);
        CLUSTER.createTopic(streamConcurrent);
        CLUSTER.createTopic(streamTwo, NUM_PARTITIONS, NUM_REPLICAS);
        CLUSTER.createTopic(streamThree, 4, 1);
        CLUSTER.createTopic(outputTopic);
        CLUSTER.createTopic(outputTopicConcurrent);
        CLUSTER.createTopic(outputTopicThree);
    }

    @Parameter
    public long cacheSizeBytes;

    //Single parameter, use Object[]
    @Parameters
    public static Object[] data() {
        return new Object[]{0, 10 * 1024 * 1024L};
    }

    @Before
    public void before() throws IOException {
        testNo++;
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = "queryable-state-" + testNo;

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("qs-test").getPath());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration
            .put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);


        stringComparator = new Comparator<KeyValue<String, String>>() {

            @Override
            public int compare(final KeyValue<String, String> o1,
                               final KeyValue<String, String> o2) {
                return o1.key.compareTo(o2.key);
            }
        };
        stringLongComparator = new Comparator<KeyValue<String, Long>>() {

            @Override
            public int compare(final KeyValue<String, Long> o1,
                               final KeyValue<String, Long> o2) {
                return o1.key.compareTo(o2.key);
            }
        };
        inputValues = Arrays.asList(
            "hello world",
            "all streams lead to kafka",
            "streams",
            "kafka streams",
            "the cat in the hat",
            "green eggs and ham",
            "that sam i am",
            "up the creek without a paddle",
            "run forest run",
            "a tank full of gas",
            "eat sleep rave repeat",
            "one jolly sailor",
            "king of the world");
        inputValuesKeys = new HashSet<>();
        for (final String sentence : inputValues) {
            final String[] words = sentence.split("\\W+");
            for (final String word : words) {
                inputValuesKeys.add(word);
            }
        }
    }

    @After
    public void shutdown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }


    /**
     * Creates a typical word count topology
     *
     * @param inputTopic
     * @param outputTopic
     * @param streamsConfiguration config
     * @return
     */
    private KafkaStreams createCountStream(final String inputTopic, final String outputTopic, final Properties streamsConfiguration) {
        final KStreamBuilder builder = new KStreamBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, inputTopic);

        final KGroupedStream<String, String> groupedByWord = textLines
            .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(final String value) {
                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                }
            })
            .groupBy(MockKeyValueMapper.<String, String>SelectValueMapper());

        // Create a State Store for the all time word count
        groupedByWord.count("word-count-store-" + inputTopic).to(Serdes.String(), Serdes.Long(), outputTopic);

        // Create a Windowed State Store that contains the word count for every 1 minute
        groupedByWord.count(TimeWindows.of(WINDOW_SIZE), "windowed-word-count-store-" + inputTopic);

        return new KafkaStreams(builder, streamsConfiguration);
    }

    private class StreamRunnable implements Runnable {
        private final KafkaStreams myStream;
        private boolean closed = false;

        StreamRunnable(final String inputTopic, final String outputTopic, final int queryPort) {
            final Properties props = (Properties) streamsConfiguration.clone();
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + queryPort);
            myStream = createCountStream(inputTopic, outputTopic, props);
        }

        @Override
        public void run() {
            myStream.start();

        }

        public void close() {
            if (!closed) {
                myStream.close();
                closed = true;
            }
        }

        public boolean isClosed() {
            return closed;
        }

        public final KafkaStreams getStream() {
            return myStream;
        }
    }

    private void verifyAllKVKeys(final StreamRunnable[] streamRunnables, final KafkaStreams streams,
                                 final Set<String> keys, final String storeName) throws Exception {
        for (final String key : keys) {
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    try {
                        final StreamsMetadata metadata = streams.metadataForKey(storeName, key, new StringSerializer());
                        if (metadata == null || metadata.equals(StreamsMetadata.NOT_AVAILABLE)) {
                            return false;
                        }
                        final int index = metadata.hostInfo().port();
                        final KafkaStreams streamsWithKey = streamRunnables[index].getStream();
                        final ReadOnlyKeyValueStore<String, Long> store = streamsWithKey.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
                        return store != null && store.get(key) != null;
                    } catch (final IllegalStateException e) {
                        // Kafka Streams instance may have closed but rebalance hasn't happened
                        return false;
                    } catch (final InvalidStateStoreException e) {
                        // rebalance
                        return false;
                    }

                }
            }, 30000, "waiting for metadata, store and value to be non null");
        }
    }


    private void verifyAllWindowedKeys(final StreamRunnable[] streamRunnables, final KafkaStreams streams,
                                       final Set<String> keys, final String storeName,
                                       final Long from, final Long to) throws Exception {
        for (final String key : keys) {
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    try {
                        final StreamsMetadata metadata = streams.metadataForKey(storeName, key, new StringSerializer());
                        if (metadata == null || metadata.equals(StreamsMetadata.NOT_AVAILABLE)) {
                            return false;
                        }
                        final int index = metadata.hostInfo().port();
                        final KafkaStreams streamsWithKey = streamRunnables[index].getStream();
                        final ReadOnlyWindowStore<String, Long> store = streamsWithKey.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
                        return store != null && store.fetch(key, from, to) != null;
                    } catch (final IllegalStateException e) {
                        // Kafka Streams instance may have closed but rebalance hasn't happened
                        return false;
                    } catch (InvalidStateStoreException e) {
                        // rebalance
                        return false;
                    }

                }
            }, 30000, "waiting for metadata, store and value to be non null");
        }
    }


    @Test
    public void queryOnRebalance() throws Exception {
        final int numThreads = NUM_PARTITIONS;
        final StreamRunnable[] streamRunnables = new StreamRunnable[numThreads];
        final Thread[] streamThreads = new Thread[numThreads];
        final int numIterations = 500000;

        // create concurrent producer
        final ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, numIterations);
        final Thread producerThread = new Thread(producerRunnable);

        // create three stream threads
        for (int i = 0; i < numThreads; i++) {
            streamRunnables[i] = new StreamRunnable(streamThree, outputTopicThree, i);
            streamThreads[i] = new Thread(streamRunnables[i]);
            streamThreads[i].start();
        }
        producerThread.start();

        try {
            waitUntilAtLeastNumRecordProcessed(outputTopicThree, 1);

            for (int i = 0; i < numThreads; i++) {
                verifyAllKVKeys(streamRunnables, streamRunnables[i].getStream(), inputValuesKeys,
                    "word-count-store-" + streamThree);
                verifyAllWindowedKeys(streamRunnables, streamRunnables[i].getStream(), inputValuesKeys,
                                      "windowed-word-count-store-" + streamThree, 0L, WINDOW_SIZE);
            }

            // kill N-1 threads
            for (int i = 1; i < numThreads; i++) {
                streamRunnables[i].close();
                streamThreads[i].interrupt();
                streamThreads[i].join();
            }

            // query from the remaining thread
            verifyAllKVKeys(streamRunnables, streamRunnables[0].getStream(), inputValuesKeys,
                "word-count-store-" + streamThree);
            verifyAllWindowedKeys(streamRunnables, streamRunnables[0].getStream(), inputValuesKeys,
                                  "windowed-word-count-store-" + streamThree, 0L, WINDOW_SIZE);
        } finally {
            for (int i = 0; i < numThreads; i++) {
                if (!streamRunnables[i].isClosed()) {
                    streamRunnables[i].close();
                    streamThreads[i].interrupt();
                    streamThreads[i].join();
                }
            }
            producerRunnable.shutdown();
            producerThread.interrupt();
            producerThread.join();
        }
    }

    @Test
    public void concurrentAccesses() throws Exception {

        final int numIterations = 500000;

        final ProducerRunnable producerRunnable = new ProducerRunnable(streamConcurrent, inputValues, numIterations);
        final Thread producerThread = new Thread(producerRunnable);
        kafkaStreams = createCountStream(streamConcurrent, outputTopicConcurrent, streamsConfiguration);

        kafkaStreams.start();
        producerThread.start();

        try {
            waitUntilAtLeastNumRecordProcessed(outputTopicConcurrent, 1);

            final ReadOnlyKeyValueStore<String, Long>
                keyValueStore = kafkaStreams.store("word-count-store-" + streamConcurrent, QueryableStoreTypes.<String, Long>keyValueStore());

            final ReadOnlyWindowStore<String, Long> windowStore =
                kafkaStreams.store("windowed-word-count-store-" + streamConcurrent, QueryableStoreTypes.<String, Long>windowStore());


            final Map<String, Long> expectedWindowState = new HashMap<>();
            final Map<String, Long> expectedCount = new HashMap<>();
            while (producerRunnable.getCurrIteration() < numIterations) {
                verifyGreaterOrEqual(inputValuesKeys.toArray(new String[inputValuesKeys.size()]), expectedWindowState,
                    expectedCount, windowStore, keyValueStore, false);
            }
            // finally check if all keys are there
            verifyGreaterOrEqual(inputValuesKeys.toArray(new String[inputValuesKeys.size()]), expectedWindowState,
                expectedCount, windowStore, keyValueStore, true);
        } finally {
            producerRunnable.shutdown();
            producerThread.interrupt();
            producerThread.join();
        }
    }

    @Test
    public void shouldBeAbleToQueryState() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
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
        s1.groupByKey().count("my-count").to(Serdes.String(), Serdes.Long(), outputTopic);

        s1.groupByKey().count(TimeWindows.of(WINDOW_SIZE), "windowed-count");
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        waitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        final ReadOnlyKeyValueStore<String, Long>
            myCount = kafkaStreams.store("my-count", QueryableStoreTypes.<String, Long>keyValueStore());

        final ReadOnlyWindowStore<String, Long> windowStore =
            kafkaStreams.store("windowed-count", QueryableStoreTypes.<String, Long>windowStore());
        verifyCanGetByKey(keys,
            expectedCount,
            expectedCount,
            windowStore,
            myCount);

        verifyRangeAndAll(expectedCount, myCount);

    }

    @Test
    public void shouldNotMakeStoreAvailableUntilAllStoresAvailable() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> stream = builder.stream(streamThree);

        final String storeName = "count-by-key";
        stream.groupByKey().count(storeName);
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

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
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                try {
                    kafkaStreams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
                    return true;
                } catch (InvalidStateStoreException ise) {
                    return false;
                }
            }
        }, maxWaitMs, "waiting for store " + storeName);

        final ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return new Long(8).equals(store.get("hello"));
            }
        }, maxWaitMs, "wait for count to be 8");

        // close stream
        kafkaStreams.close();

        // start again
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        // make sure we never get any value other than 8 for hello
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                try {
                    assertEquals(Long.valueOf(8L), kafkaStreams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore()).get("hello"));
                    return true;
                } catch (InvalidStateStoreException ise) {
                    return false;
                }
            }
        }, maxWaitMs, "waiting for store " + storeName);

    }

    private void verifyRangeAndAll(final Set<KeyValue<String, Long>> expectedCount,
                                   final ReadOnlyKeyValueStore<String, Long> myCount) {
        final Set<KeyValue<String, Long>> countRangeResults = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>> countAllResults = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>>
            expectedRangeResults =
            new TreeSet<>(stringLongComparator);

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
                                   final ReadOnlyKeyValueStore<String, Long> myCount)
        throws InterruptedException {
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

    /**
     * Verify that the new count is greater than or equal to the previous count.
     * Note: this method changes the values in expectedWindowState and expectedCount
     *
     * @param keys                  All the keys we ever expect to find
     * @param expectedWindowedCount Expected windowed count
     * @param expectedCount         Expected count
     * @param windowStore           Window Store
     * @param keyValueStore         Key-value store
     * @param failIfKeyNotFound     if true, tests fails if an expected key is not found in store. If false,
     *                              the method merely inserts the new found key into the list of
     *                              expected keys.
     * @throws InterruptedException
     */
    private void verifyGreaterOrEqual(final String[] keys,
                                      final Map<String, Long> expectedWindowedCount,
                                      final Map<String, Long> expectedCount,
                                      final ReadOnlyWindowStore<String, Long> windowStore,
                                      final ReadOnlyKeyValueStore<String, Long> keyValueStore,
                                      final boolean failIfKeyNotFound)
        throws InterruptedException {
        final Map<String, Long> windowState = new HashMap<>();
        final Map<String, Long> countState = new HashMap<>();

        for (final String key : keys) {
            final Map<String, Long> map = fetchMap(windowStore, key);
            if (map.equals(Collections.<String, Long>emptyMap()) && failIfKeyNotFound) {
                fail("Key not found " + key);
            }
            windowState.putAll(map);
            final Long value = keyValueStore.get(key);
            if (value != null) {
                countState.put(key, value);
            } else if (failIfKeyNotFound) {
                fail("Key not found " + key);
            }
        }

        for (final Map.Entry<String, Long> actualWindowStateEntry : windowState.entrySet()) {
            if (expectedWindowedCount.containsKey(actualWindowStateEntry.getKey())) {
                final Long expectedValue = expectedWindowedCount.get(actualWindowStateEntry.getKey());
                assertTrue(actualWindowStateEntry.getValue() >= expectedValue);
            }
            // return this for next round of comparisons
            expectedWindowedCount.put(actualWindowStateEntry.getKey(), actualWindowStateEntry.getValue());
        }

        for (final Map.Entry<String, Long> actualCountStateEntry : countState.entrySet()) {
            if (expectedCount.containsKey(actualCountStateEntry.getKey())) {
                final Long expectedValue = expectedCount.get(actualCountStateEntry.getKey());
                assertTrue(actualCountStateEntry.getValue() >= expectedValue);
            }
            // return this for next round of comparisons
            expectedCount.put(actualCountStateEntry.getKey(), actualCountStateEntry.getValue());
        }

    }

    private void waitUntilAtLeastNumRecordProcessed(final String topic, final int numRecs) throws InterruptedException {
        final Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            LongDeserializer.class.getName());
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
            config,
            topic,
            numRecs,
            60 * 1000);
    }

    private Set<KeyValue<String, Long>> fetch(final ReadOnlyWindowStore<String, Long> store,
                                              final String key) {

        final WindowStoreIterator<Long> fetch = store.fetch(key, 0, System.currentTimeMillis());
        if (fetch.hasNext()) {
            final KeyValue<Long, Long> next = fetch.next();
            return Collections.singleton(KeyValue.pair(key, next.value));
        }
        return Collections.emptySet();
    }

    private Map<String, Long> fetchMap(final ReadOnlyWindowStore<String, Long> store,
                                       final String key) {

        final WindowStoreIterator<Long> fetch = store.fetch(key, 0, System.currentTimeMillis());
        if (fetch.hasNext()) {
            final KeyValue<Long, Long> next = fetch.next();
            return Collections.singletonMap(key, next.value);
        }
        return Collections.emptyMap();
    }


    /**
     * A class that periodically produces records in a separate thread
     */
    private class ProducerRunnable implements Runnable {
        private final String topic;
        private final List<String> inputValues;
        private final int numIterations;
        private int currIteration = 0;
        boolean shutdown = false;

        ProducerRunnable(final String topic, final List<String> inputValues, final int numIterations) {
            this.topic = topic;
            this.inputValues = inputValues;
            this.numIterations = numIterations;
        }

        private synchronized void incrementInteration() {
            currIteration++;
        }

        public synchronized int getCurrIteration() {
            return currIteration;
        }

        public synchronized void shutdown() {
            shutdown = true;
        }

        @Override
        public void run() {
            final Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            final KafkaProducer<String, String>
                producer =
                new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer());

            while (getCurrIteration() < numIterations && !shutdown) {
                for (int i = 0; i < inputValues.size(); i++) {
                    producer.send(new ProducerRecord<String, String>(topic, inputValues.get(i)));
                }
                incrementInteration();
            }
        }
    }


}
