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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@Timeout(600)
public class RangeQueryIntegrationTest {
    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final Properties STREAMS_CONFIG = new Properties();
    private static final Long COMMIT_INTERVAL = 100L;
    private static String inputStream;
    private static final String TABLE_NAME = "mytable";
    private static final int DATA_SIZE = 5;

    private enum StoreType { InMemory, RocksDB, Timed }

    private final LinkedList<KeyValue<String, String>> records;

    private String low;
    private String high;
    private String middle;
    private String innerLow;
    private String innerHigh;
    private String innerLowBetween;
    private String innerHighBetween;

    public RangeQueryIntegrationTest() {
        records = new LinkedList<>();
        final int m = DATA_SIZE / 2;
        for (int i = 0; i < DATA_SIZE; i++) {
            final String key = "key-" + i * 2;
            final String value = "val-" + i * 2;
            records.add(new KeyValue<>(key, value));
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
        assertNotNull(low);
        assertNotNull(high);
        assertNotNull(middle);
        assertNotNull(innerLow);
        assertNotNull(innerHigh);
        assertNotNull(innerLowBetween);
        assertNotNull(innerHighBetween);
    }

    public static Stream<Arguments> data() {
        final List<StoreType> types = Arrays.asList(StoreType.InMemory, StoreType.RocksDB, StoreType.Timed);
        final List<Boolean> logging = Arrays.asList(true, false);
        final List<Boolean> caching = Arrays.asList(true, false);
        final List<Boolean> forward = Arrays.asList(true, false);
        return buildParameters(types, logging, caching, forward).stream().map(Arguments::of);
    }

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setupTopics() throws Exception {
        inputStream = "input-topic";
        CLUSTER.createTopic(inputStream);
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        CLUSTER.deleteAllTopics();
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testStoreConfig(final StoreType storeType, final boolean enableLogging, final boolean enableCaching, final boolean forward, final TestInfo testInfo) throws Exception {
        final String appID = safeUniqueTestName(testInfo);
        final StreamsBuilder builder = new StreamsBuilder();
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> stateStoreConfig = getStoreConfig(storeType, enableLogging, enableCaching);
        builder.table(inputStream, stateStoreConfig);
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), STREAMS_CONFIG)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

            writeInputData();
            final ReadOnlyKeyValueStore<String, String> stateStore = IntegrationTestUtils.getStore(1000_000L, TABLE_NAME, kafkaStreams, QueryableStoreTypes.keyValueStore());

            // wait for the store to populate
            TestUtils.waitForCondition(() -> stateStore.get(high) != null, "The store never finished populating");

            //query the state store
            try (final KeyValueIterator<String, String> scanIterator = forward ? stateStore.range(null, null) : stateStore.reverseRange(null, null)) {
                final Iterator<KeyValue<String, String>> dataIterator = forward ? records.iterator() : records.descendingIterator();
                TestUtils.checkEquals(scanIterator, dataIterator);
            }

            try (final KeyValueIterator<String, String> allIterator = forward ? stateStore.all() : stateStore.reverseAll()) {
                final Iterator<KeyValue<String, String>> dataIterator = forward ? records.iterator() : records.descendingIterator();
                TestUtils.checkEquals(allIterator, dataIterator);
            }

            testRange("range", stateStore, innerLow, innerHigh, forward);
            testRange("until", stateStore, null, middle, forward);
            testRange("from", stateStore, middle, null, forward);

            testRange("untilBetween", stateStore, null, innerHighBetween, forward);
            testRange("fromBetween", stateStore, innerLowBetween, null, forward);
        }
    }

    private void writeInputData() {
        IntegrationTestUtils.produceKeyValuesSynchronously(
                inputStream,
                records,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
        );
    }

    private List<KeyValue<String, String>> filterList(final KeyValueIterator<String, String> iterator, final String from, final String to) {
        final Predicate<KeyValue<String, String>> predicate = elem -> {
            if (from != null && elem.key.compareTo(from) < 0) {
                return false;
            }
            if (to != null && elem.key.compareTo(to) > 0) {
                return false;
            }
            return elem != null;
        };

        return Utils.toList(iterator, predicate);
    }

    private void testRange(final String name, final ReadOnlyKeyValueStore<String, String> store, final String from, final String to, final boolean forward) {
        try (final KeyValueIterator<String, String> resultIterator = forward ? store.range(from, to) : store.reverseRange(from, to);
             final KeyValueIterator<String, String> expectedIterator = forward ? store.all() : store.reverseAll()) {
            final List<KeyValue<String, String>> result = Utils.toList(resultIterator);
            final List<KeyValue<String, String>> expected = filterList(expectedIterator, from, to);
            assertThat(name, result, is(expected));
        }
    }

    private Materialized<String, String, KeyValueStore<Bytes, byte[]>> getStoreConfig(final StoreType type, final boolean cachingEnabled, final boolean loggingEnabled) {
        final Supplier<KeyValueBytesStoreSupplier> createStore = () -> {
            if (type == StoreType.InMemory) {
                return Stores.inMemoryKeyValueStore(TABLE_NAME);
            } else if (type == StoreType.RocksDB) {
                return Stores.persistentKeyValueStore(TABLE_NAME);
            } else if (type == StoreType.Timed) {
                return Stores.persistentTimestampedKeyValueStore(TABLE_NAME);
            } else {
                return Stores.inMemoryKeyValueStore(TABLE_NAME);
            }
        };

        final KeyValueBytesStoreSupplier stateStoreSupplier = createStore.get();
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> stateStoreConfig = Materialized
                .<String, String>as(stateStoreSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());
        if (cachingEnabled) {
            stateStoreConfig.withCachingEnabled();
        } else {
            stateStoreConfig.withCachingDisabled();
        }
        if (loggingEnabled) {
            stateStoreConfig.withLoggingEnabled(new HashMap<>());
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
