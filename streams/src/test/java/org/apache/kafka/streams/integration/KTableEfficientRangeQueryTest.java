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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class KTableEfficientRangeQueryTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private enum StoreType { InMemory, RocksDB, Timed }
    private static final String TABLE_NAME = "mytable";
    private static final int DATA_SIZE = 5;

    private StoreType storeType;
    private boolean enableLogging;
    private boolean enableCaching;
    private boolean forward;

    private LinkedList<KeyValue<String, String>> records;
    private String low;
    private String high;
    private String middle;
    private String innerLow;
    private String innerHigh;
    private String innerLowBetween;
    private String innerHighBetween;

    private Properties streamsConfig;

    public KTableEfficientRangeQueryTest(final StoreType storeType, final boolean enableLogging, final boolean enableCaching, final boolean forward) {
        this.storeType = storeType;
        this.enableLogging = enableLogging;
        this.enableCaching = enableCaching;
        this.forward = forward;

        this.records = new LinkedList<>();
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

    @Before
    public void setup() {
        streamsConfig = mkProperties(mkMap(
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
    }

    @Test
    public void testStoreConfig() {
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> stateStoreConfig = getStoreConfig(storeType, TABLE_NAME, enableLogging, enableCaching);
        //Create topology: table from input topic
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> table =
                builder.table("input", stateStoreConfig);
        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            //get input topic and stateStore
            final TestInputTopic<String, String> input = driver
                    .createInputTopic("input", new StringSerializer(), new StringSerializer());
            final ReadOnlyKeyValueStore<String, String> stateStore = driver.getKeyValueStore(TABLE_NAME);

            //write some data
            for (final KeyValue<String, String> kv : records) {
                input.pipeInput(kv.key, kv.value);
            }

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

    private List<KeyValue<String, String>> filterList(final KeyValueIterator<String, String> iterator, final String from, final String to) {
        final Predicate<KeyValue<String, String>> pred = new Predicate<KeyValue<String, String>>() {
            @Override
            public boolean test(final KeyValue<String, String> elem) {
                if (from != null && elem.key.compareTo(from) < 0) {
                    return false;
                }
                if (to != null && elem.key.compareTo(to) > 0) {
                    return false;
                }
                return elem != null;
            }
        };

        return Utils.toList(iterator, pred);
    }

    private void testRange(final String name, final ReadOnlyKeyValueStore<String, String> store, final String from, final String to, final boolean forward) {
        try (final KeyValueIterator<String, String> resultIterator = forward ? store.range(from, to) : store.reverseRange(from, to);
             final KeyValueIterator<String, String> expectedIterator = forward ? store.all() : store.reverseAll()) {
            final List<KeyValue<String, String>> result = Utils.toList(resultIterator);
            final List<KeyValue<String, String>> expected = filterList(expectedIterator, from, to);
            assertThat(result, is(expected));
        }
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

    private Materialized<String, String, KeyValueStore<Bytes, byte[]>> getStoreConfig(final StoreType type, final String name, final boolean cachingEnabled, final boolean loggingEnabled) {
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
            stateStoreConfig.withLoggingEnabled(new HashMap<String, String>());
        } else {
            stateStoreConfig.withLoggingDisabled();
        }
        return stateStoreConfig;
    }
}
