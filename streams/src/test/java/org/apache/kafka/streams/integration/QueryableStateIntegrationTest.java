/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.integration;

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
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
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
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
public class QueryableStateIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER =
        new EmbeddedSingleNodeKafkaCluster();
    private static final String STREAM_ONE = "stream-one";
    private static final String STREAM_CONCURRENT = "stream-concurrent";
    private static final String OUTPUT_TOPIC = "output";
    private static final String OUTPUT_TOPIC_CONCURRENT = "output-concurrent";
    private static final String STREAM_THREE = "stream-three";
    private static final int NUM_PARTITIONS = 2;
    private static final String OUTPUT_TOPIC_THREE = "output-three";
    private static final int QSRETRIES = 5;
    private static final long QSBACKOFF = 1000L;
    private Properties streamsConfiguration;
    private List<String> inputValues;
    private Set<String> inputValuesKeys;
    private KafkaStreams kafkaStreams;
    private Comparator<KeyValue<String, String>> stringComparator;
    private Comparator<KeyValue<String, Long>> stringLongComparator;

    @BeforeClass
    public static void createTopics() {
        CLUSTER.createTopic(STREAM_ONE);
        CLUSTER.createTopic(STREAM_CONCURRENT);
        CLUSTER.createTopic(STREAM_THREE, NUM_PARTITIONS, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC_CONCURRENT);
        CLUSTER.createTopic(OUTPUT_TOPIC_THREE);
    }

    @Before
    public void before() throws IOException {
        streamsConfiguration = new Properties();
        final String applicationId = "queryable-state";

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,
                                 TestUtils.tempDirectory("qs-test")
                                     .getPath());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration
            .put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

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
        inputValues = Arrays.asList("hello world",
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
        for (String sentence : inputValues) {
            String[] words = sentence.split("\\W+");
            for (String word : words) {
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
     * @param inputTopic
     * @param outputTopic
     * @param streamsConfiguration config
     * @return
     */
    private KafkaStreams createCountStream(String inputTopic, String outputTopic, Properties streamsConfiguration) {
        KStreamBuilder builder = new KStreamBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, inputTopic);

        final KGroupedStream<String, String> groupedByWord = textLines
            .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String value) {
                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                }
            })
            .groupBy(MockKeyValueMapper.<String, String>SelectValueMapper());

        // Create a State Store for the all time word count
        groupedByWord.count("word-count-store-" + inputTopic).to(Serdes.String(), Serdes.Long(), outputTopic);

        // Create a Windowed State Store that contains the word count for every 1 minute
        groupedByWord.count(TimeWindows.of(60000), "windowed-word-count-store-" + inputTopic);

        return new KafkaStreams(builder, streamsConfiguration);
    }

    private class StreamRunnable implements Runnable {
        private final KafkaStreams myStream;
        private final String inputTopic;
        private final String outputTopic;
        private final int queryPort;

        StreamRunnable(String inputTopic, String outputTopic, int queryPort) {
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
            this.queryPort = queryPort;
            Properties props = (Properties) streamsConfiguration.clone();
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + queryPort);
            this.myStream = createCountStream(inputTopic, outputTopic, props);
        }

        @Override
        public void run() {
            this.myStream.start();

        }

        public void close() {
            this.myStream.close();
        }

        public final KafkaStreams getStream() {
            return myStream;
        }
    }

    private void verifyAllKVKeys(StreamRunnable[] streamRunnables, KafkaStreams streams,
                                 Set<String> keys, Set<String> stateStores) throws Exception {
        for (String storeName : stateStores) {
            for (String key : keys) {

                // first query where the key is located
                StreamsMetadata metadata = null;
                ReadOnlyKeyValueStore<String, Long> store = null;
                Long value = null;
                int retries = 0;
                while (retries < QSRETRIES && (metadata == null || store == null || value == null)) {
                    try {
                        metadata = streams.metadataForKey(storeName, key, new StringSerializer());
                        if (metadata == null) {
                            retries++;
                            Thread.sleep(QSBACKOFF);
                            continue;
                        }
                        int index = metadata.hostInfo().port();

                        // next make sure the value is queryable
                        KafkaStreams streamsWithKey = streamRunnables[index].getStream();
                        store = streamsWithKey.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
                        if (store == null) {
                            retries++;
                            Thread.sleep(QSBACKOFF);
                            continue;
                        }
                        value = store.get(key);
                        if (value == null) {
                            retries++;
                            Thread.sleep(QSBACKOFF);
                            continue;
                        }
                        retries++;
                    } catch (Exception e) {
                        retries++;
                        Thread.sleep(QSBACKOFF);
                    }
                }
                assertThat(metadata, notNullValue());
                assertThat(store, notNullValue());
                assertThat(value, notNullValue());
            }
        }
    }

    @Test
    public void queryOnRebalance() throws Exception {
        int numThreads = 2;
        StreamRunnable[] streamRunnables = new StreamRunnable[numThreads];
        Thread[] streamThreads = new Thread[numThreads];
        final int numIterations = 500000;

        // create concurrent producer
        ProducerRunnable producerRunnable = new ProducerRunnable(STREAM_THREE, inputValues, numIterations);
        Thread producerThread = new Thread(producerRunnable);

        // create three stream threads
        for (int i = 0; i < numThreads; i++) {
            streamRunnables[i] = new StreamRunnable(STREAM_THREE, OUTPUT_TOPIC_THREE, i);
            streamThreads[i] = new Thread(streamRunnables[i]);
            streamThreads[i].start();
        }
        producerThread.start();

        waitUntilAtLeastNumRecordProcessed(OUTPUT_TOPIC_THREE, 1);

        for (int i = 0; i < numThreads; i++) {
            verifyAllKVKeys(streamRunnables, streamRunnables[i].getStream(), inputValuesKeys,
                new HashSet(Arrays.asList("word-count-store-" + STREAM_THREE)));
        }

        // kill N-1 threads
        for (int i = 1; i < numThreads; i++) {
            streamRunnables[i].close();
            streamThreads[i].interrupt();
            streamThreads[i].join();
        }

        // query from the remaining thread
        verifyAllKVKeys(streamRunnables, streamRunnables[0].getStream(), inputValuesKeys,
            new HashSet(Arrays.asList("word-count-store-" + STREAM_THREE)));
        streamRunnables[0].close();
        streamThreads[0].interrupt();
        streamThreads[0].join();

    }

    @Test
    public void concurrentAccesses() throws Exception {

        final int numIterations = 500000;

        ProducerRunnable producerRunnable = new ProducerRunnable(STREAM_CONCURRENT, inputValues, numIterations);
        Thread producerThread = new Thread(producerRunnable);
        kafkaStreams = createCountStream(STREAM_CONCURRENT, OUTPUT_TOPIC_CONCURRENT, streamsConfiguration);
        kafkaStreams.start();
        producerThread.start();

        waitUntilAtLeastNumRecordProcessed(OUTPUT_TOPIC_CONCURRENT, 1);

        final ReadOnlyKeyValueStore<String, Long>
            myCount = kafkaStreams.store("word-count-store-" + STREAM_CONCURRENT, QueryableStoreTypes.<String, Long>keyValueStore());

        final ReadOnlyWindowStore<String, Long> windowStore =
            kafkaStreams.store("windowed-word-count-store-" + STREAM_CONCURRENT, QueryableStoreTypes.<String, Long>windowStore());


        Map<String, Long> expectedWindowState = new HashMap<>();
        Map<String, Long> expectedCount = new HashMap<>();
        while (producerRunnable.getCurrIteration() < numIterations) {
            verifyGreaterOrEqual(inputValuesKeys.toArray(new String[inputValuesKeys.size()]), expectedWindowState, expectedCount, windowStore, myCount, false);
        }
        // finally check if all keys are there
        verifyGreaterOrEqual(inputValuesKeys.toArray(new String[inputValuesKeys.size()]), expectedWindowState, expectedCount, windowStore, myCount, true);

    }

    @Test
    public void shouldBeAbleToQueryState() throws Exception {
        KStreamBuilder builder = new KStreamBuilder();
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};

        final Set<KeyValue<String, String>> batch1 = new TreeSet<>(stringComparator);
        batch1.addAll(Arrays.asList(
            new KeyValue<>(keys[0], "hello"),
            new KeyValue<>(keys[1], "goodbye"),
            new KeyValue<>(keys[2], "welcome"),
            new KeyValue<>(keys[3], "go"),
            new KeyValue<>(keys[4], "kafka")));


        final Set<KeyValue<String, Long>> expectedCount = new TreeSet<>(stringLongComparator);
        for (String key : keys) {
            expectedCount.add(new KeyValue<>(key, 1L));
        }

        IntegrationTestUtils.produceKeyValuesSynchronously(
            STREAM_ONE,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()));

        final KStream<String, String> s1 = builder.stream(STREAM_ONE);

        // Non Windowed
        s1.groupByKey().count("my-count").to(Serdes.String(), Serdes.Long(), OUTPUT_TOPIC);

        s1.groupByKey().count(TimeWindows.of(60000L), "windowed-count");
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        waitUntilAtLeastNumRecordProcessed(OUTPUT_TOPIC, 1);

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
        while (windowState.size() < 5 &&
               countState.size() < 5 &&
               System.currentTimeMillis() < timeout) {
            Thread.sleep(10);
            for (String key : keys) {
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
     * Note: this method changes the values in expectedWindowState and expectedCount
     * @param keys All the keys we ever expect to find
     * @param expectedWindowState Expected window state
     * @param expectedCount Expected cound
     * @param windowStore Window Store
     * @param myCount CountStore
     * @param failIfKeyNotFound if true, tests fails if an expected key is not found in store. If false,
     *                          the method merely inserts the new found key into the list of
     *                          expected keys.
     * @throws InterruptedException
     */
    private void verifyGreaterOrEqual(final String[] keys,
                                      Map<String, Long> expectedWindowState,
                                      Map<String, Long> expectedCount,
                                      final ReadOnlyWindowStore<String, Long> windowStore,
                                      final ReadOnlyKeyValueStore<String, Long> myCount,
                                      boolean failIfKeyNotFound)
        throws InterruptedException {
        final Map<String, Long> windowState = new HashMap<>();
        final Map<String, Long> countState = new HashMap<>();

        for (String key : keys) {
            Map<String, Long> map = fetchMap(windowStore, key);
            if (map.equals(Collections.<String, Long>emptyMap()) && failIfKeyNotFound) {
                fail("Key not found " + key);
            }
            windowState.putAll(map);
            final Long value = myCount.get(key);
            if (value != null) {
                countState.put(key, value);
            } else {
                if (failIfKeyNotFound) {
                    fail("Key not found " + key);
                }
            }
        }

        for (Map.Entry<String, Long> actualWindowStateEntry : windowState.entrySet()) {
            if (expectedWindowState.containsKey(actualWindowStateEntry.getKey())) {
                Long expectedValue = expectedWindowState.get(actualWindowStateEntry.getKey());
                assertTrue(actualWindowStateEntry.getValue() >= expectedValue);
            } else {
                if (failIfKeyNotFound) {
                    fail("Key not found " + actualWindowStateEntry.getKey());
                }
            }
            // return this for next round of comparisons
            expectedWindowState.put(actualWindowStateEntry.getKey(), actualWindowStateEntry.getValue());
        }

        for (Map.Entry<String, Long> actualCountStateEntry : countState.entrySet()) {
            if (expectedCount.containsKey(actualCountStateEntry.getKey())) {
                Long expectedValue = expectedCount.get(actualCountStateEntry.getKey());
                assertTrue(actualCountStateEntry.getValue() >= expectedValue);
            } else {
                if (failIfKeyNotFound) {
                    fail("Key not found " + actualCountStateEntry.getKey());
                }
            }
            // return this for next round of comparisons
            expectedCount.put(actualCountStateEntry.getKey(), actualCountStateEntry.getValue());
        }

    }

    private void waitUntilAtLeastNumRecordProcessed(String topic, int numRecs) throws InterruptedException {
        final Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                           StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                           LongDeserializer.class.getName());
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(config,
                                                               topic,
                                                               numRecs,
                                                               60 *
                                                               1000);
    }

    private Set<KeyValue<String, Long>> fetch(final ReadOnlyWindowStore<String, Long> store,
                                                final String key) {

        final WindowStoreIterator<Long> fetch = store.fetch(key, 0, System.currentTimeMillis());
        if (fetch.hasNext()) {
            KeyValue<Long, Long> next = fetch.next();
            return Collections.singleton(KeyValue.pair(key, next.value));
        }
        return Collections.emptySet();
    }

    private Map<String, Long> fetchMap(final ReadOnlyWindowStore<String, Long> store,
                                       final String key) {

        final WindowStoreIterator<Long> fetch = store.fetch(key, 0, System.currentTimeMillis());
        if (fetch.hasNext()) {
            KeyValue<Long, Long> next = fetch.next();
            return Collections.singletonMap(key, next.value);
        }
        return Collections.emptyMap();
    }


    /**
     * A class that periodically produces records in a separate thread
     */
    private class ProducerRunnable implements Runnable {
        private String topic;
        private boolean keepRunning = false;
        private final List<String> inputValues;
        private final int numIterations;
        private int currIteration = 0;

        private final Random random = new Random();

        ProducerRunnable(String topic, List<String> inputValues, int numIterations) {
            this.topic = topic;
            this.inputValues = inputValues;
            this.numIterations = numIterations;
        }

        public synchronized void incrementInteration() {
            currIteration++;
        }
        public synchronized int getCurrIteration() {
            return currIteration;
        }

        @Override
        public void run() {
            keepRunning = true;
            Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            final KafkaProducer<String, String>
                producer =
                new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer());

            try {
                while (keepRunning && getCurrIteration() < numIterations) {
                    for (int i = 0; i < inputValues.size(); i++) {
                        producer.send(new ProducerRecord<>(topic,
                            inputValues.get(i), inputValues.get(i)));
                    }
                    Thread.sleep(0L);
                    incrementInteration();
                }
            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
            }
        }
    }


}
