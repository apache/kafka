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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class RangeQueryForWindowStoreIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final Properties STREAMS_CONFIG = new Properties();
    private static final String APP_ID = "range-query-integration-test";
    private static final Long COMMIT_INTERVAL = 100L;
    private static final String INPUT_STREAM = "input";
    private static final String STORE_NAME = "store";
    private static final int DATA_SIZE = 5;
    private static final long WINDOW_SIZE = 500L;
    private static final long RETENTION_MS = 300000L;
    private static int storeNameCount;

    private enum StoreType { InMemory, RocksDB, Timed };
    private StoreType storeType;
    private boolean enableLogging;
    private boolean enableCaching;
    private boolean forward;
    private KafkaStreams kafkaStreams;

    private LinkedList<KeyValue<Windowed<String>, Long>> expectedRecords;
    private LinkedList<KeyValue<String, String>> records;
    private String low;
    private String high;
    private String middle;
    private String innerLow;
    private String innerHigh;
    private String innerLowBetween;
    private String innerHighBetween;
    private String storeName;

    public RangeQueryForWindowStoreIntegrationTest(final StoreType storeType, final boolean enableLogging, final boolean enableCaching, final boolean forward) {
        this.storeType = storeType;
        this.enableLogging = enableLogging;
        this.enableCaching = enableCaching;
        this.forward = forward;

        this.records = new LinkedList<>();
        this.expectedRecords = new LinkedList<>();
        final int m = DATA_SIZE / 2;
        for (int i = 0; i < DATA_SIZE; i++) {
            final String key = "key-" + i * 2;
            final String value = "val-" + i * 2;
            final KeyValue<String, String> r = new KeyValue<>(key, value);
            records.add(r);
            records.add(r);
            // expected the count of each key is 2
            expectedRecords.add(new KeyValue<>(new Windowed<>(key, new TimeWindow(0L, WINDOW_SIZE)), 2L));
            high = key;
            if (low == null) {
                low = key;
            }
            if (i == m) {
                middle = key;
            }
            if (i == 1) {
                innerLow = key;
                final int index = i * 2 - 1;
                innerLowBetween = "key-" + index;
            }
            if (i == DATA_SIZE - 2) {
                innerHigh = key;
                final int index = i * 2 + 1;
                innerHighBetween = "key-" + index;
            }
        }
        Assert.assertNotNull(low);
        Assert.assertNotNull(high);
        Assert.assertNotNull(middle);
        Assert.assertNotNull(innerLow);
        Assert.assertNotNull(innerHigh);
        Assert.assertNotNull(innerLowBetween);
        Assert.assertNotNull(innerHighBetween);
    }

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameters(name = "storeType={0}, enableLogging={1}, enableCaching={2}, forward={3}")
    public static Collection<Object[]> data() {
        final List<StoreType> types = Arrays.asList(StoreType.InMemory, StoreType.RocksDB, StoreType.Timed);
        final List<Boolean> logging = Arrays.asList(true, false);
        final List<Boolean> caching = Arrays.asList(true, false);
        final List<Boolean> forward = Arrays.asList(true, false);
        return buildParameters(types, logging, caching, forward);
    }

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void setupTopics() throws Exception {
        CLUSTER.createTopic(INPUT_STREAM);
        storeName = STORE_NAME + storeNameCount++;
    }

    @After
    public void cleanup() throws InterruptedException {
        CLUSTER.deleteAllTopicsAndWait(120000);
    }

    @Test
    public void testStoreConfig() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final Materialized<String, Long, WindowStore<Bytes, byte[]>> stateStoreConfig = getStoreConfig(storeType, storeName, enableLogging, enableCaching);
        final KStream<String, String> stream = builder.stream(INPUT_STREAM, Consumed.with(Serdes.String(), Serdes.String()));
        stream.
            groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(ofMillis(WINDOW_SIZE)))
            .count(stateStoreConfig)
            .toStream()
            .to("output");


        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), STREAMS_CONFIG)) {
            final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams);

            IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

            writeInputData();

            final ReadOnlyWindowStore<String, Long> stateStore = IntegrationTestUtils.getStore(1000_000L, storeName, kafkaStreams, QueryableStoreTypes.windowStore());

            TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                //query the state store
                try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
                    stateStore.fetch(null, null, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)) :
                    stateStore.backwardFetch(null, null, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))) {

                    final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                        expectedRecords.iterator() :
                        expectedRecords.descendingIterator();

                    TestUtils.checkEquals(scanIterator, dataIterator);
                }
            });

            testRange("range", stateStore, innerLow, innerHigh, forward);
            testRange("until", stateStore, null, middle, forward);
            testRange("from", stateStore, middle, null, forward);

            testRange("untilBetween", stateStore, null, innerHighBetween, forward);
            testRange("fromBetween", stateStore, innerLowBetween, null, forward);
        }
    }

    private void writeInputData() {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            INPUT_STREAM,
            records,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            0L
        );
    }

    private List<KeyValue<Windowed<String>, Long>> filterList(final KeyValueIterator<Windowed<String>, Long> iterator, final String from, final String to) {
        final Predicate<KeyValue<Windowed<String>, Long>> pred = new Predicate<KeyValue<Windowed<String>, Long>>() {
            @Override
            public boolean test(final KeyValue<Windowed<String>, Long> elem) {
                if (from != null && elem.key.key().compareTo(from) < 0) {
                    return false;
                }
                if (to != null && elem.key.key().compareTo(to) > 0) {
                    return false;
                }
                return elem != null;
            }
        };

        return Utils.toList(iterator, pred);
    }

    private void testRange(final String name, final ReadOnlyWindowStore<String, Long> store, final String from, final String to, final boolean forward) {
        try (final KeyValueIterator<Windowed<String>, Long> resultIterator = forward ?
            store.fetch(from, to, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)) :
            store.backwardFetch(from, to, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE));
             final KeyValueIterator<Windowed<String>, Long> expectedIterator = forward ? store.all() : store.backwardAll()) {
            final List<KeyValue<Windowed<String>, Long>> result = Utils.toList(resultIterator);
            final List<KeyValue<Windowed<String>, Long>> expected = filterList(expectedIterator, from, to);
            assertThat(result, is(expected));
        }
    }

//    private Materialized<String, String, KeyValueStore<Bytes, byte[]>> getStoreConfig(final StoreType type, final String name, final boolean cachingEnabled, final boolean loggingEnabled) {
    private Materialized<String, Long, WindowStore<Bytes, byte[]>> getStoreConfig(final StoreType type, final String name, final boolean cachingEnabled, final boolean loggingEnabled) {
        final Supplier<WindowBytesStoreSupplier> createStore = () -> {
            if (type == StoreType.InMemory) {
                return Stores.inMemoryWindowStore(name, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            } else if (type == StoreType.RocksDB) {
                return Stores.persistentWindowStore(name, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            } else if (type == StoreType.Timed) {
                return Stores.persistentTimestampedWindowStore(name, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            } else {
                return Stores.inMemoryWindowStore(name, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            }
        };

        final WindowBytesStoreSupplier stateStoreSupplier = createStore.get();
        final Materialized<String, Long, WindowStore<Bytes, byte[]>> stateStoreConfig = Materialized
            .<String, Long>as(stateStoreSupplier)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long());
        if (cachingEnabled) {
            stateStoreConfig.withCachingEnabled();
        } else {
            stateStoreConfig.withCachingDisabled();
        }
        if (loggingEnabled) {
            stateStoreConfig.withLoggingEnabled(new HashMap<String, String>());
        } else {
            stateStoreConfig.withLoggingDisabled();
        }
        return stateStoreConfig;
    }

    private static Collection<Object[]> buildParameters(final List<?>... argOptions) {
        List<Object[]> result = new LinkedList<>();
        result.add(new Object[0]);

        for (final List<?> argOption : argOptions) {
            result = times(result, argOption);
        }

        return result;
    }

    private static List<Object[]> times(final List<Object[]> left, final List<?> right) {
        final List<Object[]> result = new LinkedList<>();
        for (final Object[] args : left) {
            for (final Object rightElem : right) {
                final Object[] resArgs = new Object[args.length + 1];
                System.arraycopy(args, 0, resArgs, 0, args.length);
                resArgs[args.length] = rightElem;
                result.add(resArgs);
            }
        }
        return result;
    }
}
