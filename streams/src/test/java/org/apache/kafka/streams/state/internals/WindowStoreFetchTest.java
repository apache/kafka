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

package org.apache.kafka.streams.state.internals;

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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class WindowStoreFetchTest {
    private enum StoreType { InMemory, RocksDB, Timed }
    private static final String STORE_NAME = "store";
    private static final int DATA_SIZE = 5;
    private static final long WINDOW_SIZE = 500L;
    private static final long RETENTION_MS = 10000L;

    private StoreType storeType;
    private boolean enableLogging;
    private boolean enableCaching;
    private boolean forward;

    private LinkedList<KeyValue<Windowed<String>, Long>> expectedRecords;
    private LinkedList<KeyValue<String, String>> records;
    private Properties streamsConfig;
    private String low;
    private String high;
    private String middle;
    private String innerLow;
    private String innerHigh;
    private String innerLowBetween;
    private String innerHighBetween;
    private String storeName;

    private TimeWindowedKStream<String, String> windowedStream;

    public WindowStoreFetchTest(final StoreType storeType, final boolean enableLogging, final boolean enableCaching, final boolean forward) {
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
            final long windowStartTime = i < m ? 0 : WINDOW_SIZE;
            expectedRecords.add(new KeyValue<>(new Windowed<>(key, new TimeWindow(windowStartTime, windowStartTime + WINDOW_SIZE)), 2L));
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
        final Materialized<String, Long, WindowStore<Bytes, byte[]>> stateStoreConfig = getStoreConfig(storeType, STORE_NAME, enableLogging, enableCaching);
        //Create topology: table from input topic
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));
        stream.
            groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(WINDOW_SIZE)))
            .count(stateStoreConfig)
            .toStream()
            .to("output");

        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            //get input topic and stateStore
            final TestInputTopic<String, String> input = driver
                    .createInputTopic("input", new StringSerializer(), new StringSerializer());
            final WindowStore<String, Long> stateStore = driver.getWindowStore(STORE_NAME);

            //write some data
            final int medium = DATA_SIZE / 2 * 2;
            for (int i = 0; i < records.size(); i++) {
                final KeyValue<String, String> kv = records.get(i);
                final long windowStartTime = i < medium ? 0 : WINDOW_SIZE;
                input.pipeInput(kv.key, kv.value, windowStartTime + i);
            }

            // query the state store
            try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
                stateStore.fetchAll(0, Long.MAX_VALUE) :
                stateStore.backwardFetchAll(0, Long.MAX_VALUE)) {

                final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                    expectedRecords.iterator() :
                    expectedRecords.descendingIterator();

                TestUtils.checkEquals(scanIterator, dataIterator);
            }

            try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
                stateStore.fetch(null, null, 0, Long.MAX_VALUE) :
                stateStore.backwardFetch(null, null, 0, Long.MAX_VALUE)) {

                final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                    expectedRecords.iterator() :
                    expectedRecords.descendingIterator();

                TestUtils.checkEquals(scanIterator, dataIterator);
            }

            testRange("range", stateStore, innerLow, innerHigh, forward);
            testRange("until", stateStore, null, middle, forward);
            testRange("from", stateStore, middle, null, forward);

            testRange("untilBetween", stateStore, null, innerHighBetween, forward);
            testRange("fromBetween", stateStore, innerLowBetween, null, forward);
        }
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

    private void testRange(final String name, final WindowStore<String, Long> store, final String from, final String to, final boolean forward) {
        try (final KeyValueIterator<Windowed<String>, Long> resultIterator = forward ? store.fetch(from, to, 0, Long.MAX_VALUE) : store.backwardFetch(from, to, 0, Long.MAX_VALUE);
             final KeyValueIterator<Windowed<String>, Long> expectedIterator = forward ? store.fetchAll(0, Long.MAX_VALUE) : store.backwardFetchAll(0, Long.MAX_VALUE)) {
            final List<KeyValue<Windowed<String>, Long>> result = Utils.toList(resultIterator);
            final List<KeyValue<Windowed<String>, Long>> expected = filterList(expectedIterator, from, to);
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

    private Materialized<String, Long, WindowStore<Bytes, byte[]>> getStoreConfig(final StoreType type, final String name, final boolean cachingEnabled, final boolean loggingEnabled) {
        final Supplier<WindowBytesStoreSupplier> createStore = () -> {
            if (type == StoreType.InMemory) {
                return Stores.inMemoryWindowStore(STORE_NAME, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            } else if (type == StoreType.RocksDB) {
                return Stores.persistentWindowStore(STORE_NAME, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            } else if (type == StoreType.Timed) {
                return Stores.persistentTimestampedWindowStore(STORE_NAME, Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE),
                    false);
            } else {
                return Stores.inMemoryWindowStore(STORE_NAME, Duration.ofMillis(RETENTION_MS),
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
}
