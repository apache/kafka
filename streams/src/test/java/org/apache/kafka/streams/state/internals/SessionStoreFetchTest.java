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
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
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
public class SessionStoreFetchTest {
    private enum StoreType { InMemory, RocksDB };
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

    public SessionStoreFetchTest(final StoreType storeType, final boolean enableLogging, final boolean enableCaching, final boolean forward) {
        this.storeType = storeType;
        this.enableLogging = enableLogging;
        this.enableCaching = enableCaching;
        this.forward = forward;

        this.records = new LinkedList<>();
        this.expectedRecords = new LinkedList<>();
        final int m = DATA_SIZE / 2;
        for (int i = 0; i < DATA_SIZE; i++) {
            final String keyStr = i < m ? "a" : "b";
            final String key = "key-" + keyStr;
            final String key2 = "key-" + keyStr + keyStr;
            final String value = "val-" + i;
            final KeyValue<String, String> r = new KeyValue<>(key, value);
            final KeyValue<String, String> r2 = new KeyValue<>(key2, value);
            records.add(r);
            records.add(r2);
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

        expectedRecords.add(new KeyValue<>(new Windowed<>("key-a", new SessionWindow(0, 500)), 4L));
        expectedRecords.add(new KeyValue<>(new Windowed<>("key-aa", new SessionWindow(0, 500)), 4L));
        expectedRecords.add(new KeyValue<>(new Windowed<>("key-b", new SessionWindow(1500, 2000)), 6L));
        expectedRecords.add(new KeyValue<>(new Windowed<>("key-bb", new SessionWindow(1500, 2000)), 6L));
    }

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameters(name = "storeType={0}, enableLogging={1}, enableCaching={2}, forward={3}")
    public static Collection<Object[]> data() {
        final List<StoreType> types = Arrays.asList(StoreType.InMemory, StoreType.RocksDB);
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

    private void verifyNormalQuery(final SessionStore<String, Long> stateStore) {
        try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
            stateStore.fetch("key-a", "key-bb") :
            stateStore.backwardFetch("key-a", "key-bb")) {

            final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                expectedRecords.iterator() :
                expectedRecords.descendingIterator();

            TestUtils.checkEquals(scanIterator, dataIterator);
        }

        try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
            stateStore.findSessions("key-a", "key-bb", 0L, Long.MAX_VALUE) :
            stateStore.backwardFindSessions("key-a", "key-bb", 0L, Long.MAX_VALUE)) {

            final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                expectedRecords.iterator() :
                expectedRecords.descendingIterator();

            TestUtils.checkEquals(scanIterator, dataIterator);
        }
    }

    private void verifyInfiniteQuery(final SessionStore<String, Long> stateStore) {
        try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
            stateStore.fetch(null, null) :
            stateStore.backwardFetch(null, null)) {

            final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                expectedRecords.iterator() :
                expectedRecords.descendingIterator();

            TestUtils.checkEquals(scanIterator, dataIterator);
        }

        try (final KeyValueIterator<Windowed<String>, Long> scanIterator = forward ?
            stateStore.findSessions(null, null, 0L, Long.MAX_VALUE) :
            stateStore.backwardFindSessions(null, null, 0L, Long.MAX_VALUE)) {

            final Iterator<KeyValue<Windowed<String>, Long>> dataIterator = forward ?
                expectedRecords.iterator() :
                expectedRecords.descendingIterator();

            TestUtils.checkEquals(scanIterator, dataIterator);
        }
    }

    private void verifyRangeQuery(final SessionStore<String, Long> stateStore) {
        testRange("range", stateStore, innerLow, innerHigh, forward);
        testRange("until", stateStore, null, middle, forward);
        testRange("from", stateStore, middle, null, forward);

        testRange("untilBetween", stateStore, null, innerHighBetween, forward);
        testRange("fromBetween", stateStore, innerLowBetween, null, forward);
    }

    @Test
    public void testStoreConfig() {
        final Materialized<String, Long, SessionStore<Bytes, byte[]>> stateStoreConfig = getStoreConfig(storeType, STORE_NAME, enableLogging, enableCaching);
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));
        stream.
            groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(WINDOW_SIZE)))
            .count(stateStoreConfig)
            .toStream()
            .to("output");

        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            //get input topic and stateStore
            final TestInputTopic<String, String> input = driver
                    .createInputTopic("input", new StringSerializer(), new StringSerializer());
            final SessionStore<String, Long> stateStore = driver.getSessionStore(STORE_NAME);

            //write some data
            final int medium = DATA_SIZE / 2 * 2;
            for (int i = 0; i < records.size(); i++) {
                final KeyValue<String, String> kv = records.get(i);
                final long windowStartTime = i < medium ? 0 : 1500;
                input.pipeInput(kv.key, kv.value, windowStartTime);
                input.pipeInput(kv.key, kv.value, windowStartTime + WINDOW_SIZE);
            }

            verifyNormalQuery(stateStore);
            verifyInfiniteQuery(stateStore);
            verifyRangeQuery(stateStore);
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

    private void testRange(final String name, final SessionStore<String, Long> store, final String from, final String to, final boolean forward) {
        try (final KeyValueIterator<Windowed<String>, Long> resultIterator = forward ? store.fetch(from, to) : store.backwardFetch(from, to);
             final KeyValueIterator<Windowed<String>, Long> expectedIterator = forward ? store.fetch(null, null) : store.backwardFetch(null, null)) {
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

    private Materialized<String, Long, SessionStore<Bytes, byte[]>> getStoreConfig(final StoreType type, final String name, final boolean cachingEnabled, final boolean loggingEnabled) {
        final Supplier<SessionBytesStoreSupplier> createStore = () -> {
            if (type == StoreType.InMemory) {
                return Stores.inMemorySessionStore(STORE_NAME, Duration.ofMillis(RETENTION_MS));
            } else if (type == StoreType.RocksDB) {
                return Stores.persistentSessionStore(STORE_NAME, Duration.ofMillis(RETENTION_MS));
            } else {
                return Stores.inMemorySessionStore(STORE_NAME, Duration.ofMillis(RETENTION_MS));
            }
        };

        final SessionBytesStoreSupplier stateStoreSupplier = createStore.get();
        final Materialized<String, Long, SessionStore<Bytes, byte[]>> stateStoreConfig = Materialized
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
