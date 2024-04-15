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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.toList;
import static org.apache.kafka.test.StreamsTestUtils.valuesToSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public abstract class AbstractSessionBytesStoreTest {

    static final long SEGMENT_INTERVAL = 60_000L;
    static final long RETENTION_PERIOD = 10_000L;

    enum StoreType {
        RocksDBSessionStore,
        RocksDBTimeOrderedSessionStoreWithIndex,
        RocksDBTimeOrderedSessionStoreWithoutIndex,
        InMemoryStore
    }

    SessionStore<String, Long> sessionStore;

    private MockRecordCollector recordCollector;

    InternalMockProcessorContext context;

    abstract <K, V> SessionStore<K, V> buildSessionStore(final long retentionPeriod,
                                                         final Serde<K> keySerde,
                                                         final Serde<V> valueSerde);

    abstract StoreType getStoreType();

    @Before
    public void setUp() {
        sessionStore = buildSessionStore(RETENTION_PERIOD, Serdes.String(), Serdes.Long());
        recordCollector = new MockRecordCollector();
        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));
        context.setTime(1L);

        sessionStore.init((StateStoreContext) context, sessionStore);
    }

    @After
    public void after() {
        sessionStore.close();
    }

    @Test
    public void shouldPutAndFindSessionsInRange() {
        final String key = "a";
        final Windowed<String> a1 = new Windowed<>(key, new SessionWindow(10, 10L));
        final Windowed<String> a2 = new Windowed<>(key, new SessionWindow(500L, 1000L));
        sessionStore.put(a1, 1L);
        sessionStore.put(a2, 2L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1500L, 2000L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(2500L, 3000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected =
            Arrays.asList(KeyValue.pair(a1, 1L), KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(key, 0, 1000L)
        ) {
            assertEquals(expected, toList(values));
        }

        final List<KeyValue<Windowed<String>, Long>> expected2 =
            Collections.singletonList(KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values2 = sessionStore.findSessions(key, 400L, 600L)
        ) {
            assertEquals(expected2, toList(values2));
        }
    }

    @Test
    public void shouldPutAndBackwardFindSessionsInRange() {
        final String key = "a";
        final Windowed<String> a1 = new Windowed<>(key, new SessionWindow(10, 10L));
        final Windowed<String> a2 = new Windowed<>(key, new SessionWindow(500L, 1000L));
        sessionStore.put(a1, 1L);
        sessionStore.put(a2, 2L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1500L, 2000L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(2500L, 3000L)), 2L);

        final LinkedList<KeyValue<Windowed<String>, Long>> expected = new LinkedList<>();
        expected.add(KeyValue.pair(a1, 1L));
        expected.add(KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFindSessions(key, 0, 1000L)) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }

        final List<KeyValue<Windowed<String>, Long>> expected2 =
            Collections.singletonList(KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values2 = sessionStore.backwardFindSessions(key, 400L, 600L)) {
            assertEquals(expected2, toList(values2));
        }
    }

    @Test
    public void shouldFetchAllSessionsWithSameRecordKey() {
        final LinkedList<KeyValue<Windowed<String>, Long>> expected = new LinkedList<>();
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L));
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L));
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L));
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldFindSessionsForTimeRange() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 5L);

        if (getStoreType() == StoreType.RocksDBSessionStore) {
            assertThrows(
                "This API is not supported by this implementation of SessionStore.",
                UnsupportedOperationException.class,
                () -> sessionStore.findSessions(0, 0)
            );
            return;
        }

        // Find point
        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(0, 0)) {
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 5L)
            );
            assertEquals(expected, toList(values));
        }

        sessionStore.put(new Windowed<>("b", new SessionWindow(10, 20)), 10L);
        sessionStore.put(new Windowed<>("c", new SessionWindow(30, 40)), 20L);

        // Find boundary
        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(0, 20)) {
            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 5L),
                KeyValue.pair(new Windowed<>("b", new SessionWindow(10, 20)), 10L)
            );
            assertEquals(expected, toList(values));
        }

        // Find left boundary
        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(0, 19)) {
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 5L)
            );
            assertEquals(expected, toList(values));
        }

        // Find right boundary
        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(1, 20)) {
            final List<KeyValue<Windowed<String>, Long>> expected = Collections.singletonList(
                KeyValue.pair(new Windowed<>("b", new SessionWindow(10, 20)), 10L)
            );
            assertEquals(expected, toList(values));
        }

        // Find partial off by 1
        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(19, 41)) {
            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>("b", new SessionWindow(10, 20)), 10L),
                KeyValue.pair(new Windowed<>("c", new SessionWindow(30, 40)), 20L)
            );
            assertEquals(expected, toList(values));
        }

        // Find all boundary
        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(0, 40)) {
            final List<KeyValue<Windowed<String>, Long>> expected = asList(
                KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 5L),
                KeyValue.pair(new Windowed<>("b", new SessionWindow(10, 20)), 10L),
                KeyValue.pair(new Windowed<>("c", new SessionWindow(30, 40)), 20L)
            );
            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldBackwardFetchAllSessionsWithSameRecordKey() {
        final LinkedList<KeyValue<Windowed<String>, Long>> expected = new LinkedList<>();
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L));
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L));
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L));
        expected.add(KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch("a")) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }
    }

    @Test
    public void shouldFetchAllSessionsWithinKeyRange() {
        final List<KeyValue<Windowed<String>, Long>> expected = new LinkedList<>();
        expected.add(KeyValue.pair(new Windowed<>("aa", new SessionWindow(10, 10)), 2L));
        expected.add(KeyValue.pair(new Windowed<>("aaa", new SessionWindow(100, 100)), 3L));
        expected.add(KeyValue.pair(new Windowed<>("aaaa", new SessionWindow(100, 100)), 6L));
        expected.add(KeyValue.pair(new Windowed<>("b", new SessionWindow(1000, 1000)), 4L));
        expected.add(KeyValue.pair(new Windowed<>("bb", new SessionWindow(1500, 2000)), 5L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add some that should only be fetched in infinite fetch
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("aa", "bb")) {
            assertEquals(expected, toList(values));
        }

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions("aa", "bb", 0L, Long.MAX_VALUE)) {
            assertEquals(expected, toList(values));
        }

        // infinite keyFrom fetch case
        expected.add(0, KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch(null, "bb")) {
            assertEquals(expected, toList(values));
        }

        // remove the one added for unlimited start fetch case
        expected.remove(0);
        // infinite keyTo fetch case
        expected.add(KeyValue.pair(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("aa", null)) {
            assertEquals(expected, toList(values));
        }

        // fetch all case
        expected.add(0, KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch(null, null)) {
            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldBackwardFetchAllSessionsWithinKeyRange() {
        final LinkedList<KeyValue<Windowed<String>, Long>> expected = new LinkedList<>();
        expected.add(KeyValue.pair(new Windowed<>("aa", new SessionWindow(10, 10)), 2L));
        expected.add(KeyValue.pair(new Windowed<>("aaa", new SessionWindow(100, 100)), 3L));
        expected.add(KeyValue.pair(new Windowed<>("aaaa", new SessionWindow(100, 100)), 6L));
        expected.add(KeyValue.pair(new Windowed<>("b", new SessionWindow(1000, 1000)), 4L));
        expected.add(KeyValue.pair(new Windowed<>("bb", new SessionWindow(1500, 2000)), 5L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add some that should only be fetched in infinite fetch
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch("aa", "bb")) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFindSessions("aa", "bb", 0L, Long.MAX_VALUE)) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }

        // infinite keyFrom fetch case
        expected.add(0, KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch(null, "bb")) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }

        // remove the one added for unlimited start fetch case
        expected.remove(0);
        // infinite keyTo fetch case
        expected.add(KeyValue.pair(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch("aa", null)) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }

        // fetch all case
        expected.add(0, KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch(null, null)) {
            assertEquals(toList(expected.descendingIterator()), toList(values));
        }
    }

    @Test
    public void shouldFetchExactSession() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 4)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 3)), 2L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 4)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(1, 4)), 4L);
        sessionStore.put(new Windowed<>("aaa", new SessionWindow(0, 4)), 5L);

        final long result = sessionStore.fetchSession("aa", 0, 4);
        assertEquals(3L, result);
    }

    @Test
    public void shouldReturnNullOnSessionNotFound() {
        assertNull(sessionStore.fetchSession("any key", 0L, 5L));
    }

    @Test
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions(key, -1, 1000L)) {
            assertEquals(expected, toList(results));
        }
    }

    @Test
    public void shouldBackwardFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final LinkedList<KeyValue<Windowed<String>, Long>> expected = new LinkedList<>();
        expected.add(KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L));
        expected.add(KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.backwardFindSessions(key, -1, 1000L)) {
            assertEquals(toList(expected.descendingIterator()), toList(results));
        }
    }

    @Test
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }

    @Test
    public void shouldRemoveOnNullAggValue() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), null);

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }

    @Test
    public void shouldFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        final List<KeyValue<Windowed<String>, Long>> expected =
            Arrays.asList(KeyValue.pair(session2, 2L), KeyValue.pair(session3, 3L));

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 150, 300)) {
            assertEquals(expected, toList(results));
        }
    }

    @Test
    public void shouldBackwardFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        final List<KeyValue<Windowed<String>, Long>> expected =
            asList(KeyValue.pair(session3, 3L), KeyValue.pair(session2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.backwardFindSessions("a", 150, 300)) {
            assertEquals(expected, toList(results));
        }
    }

    @Test
    public void shouldFetchExactKeys() {
        sessionStore.close();
        sessionStore = buildSessionStore(0x7a00000000000000L, Serdes.String(), Serdes.Long());
        sessionStore.init((StateStoreContext) context, sessionStore);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);
        sessionStore.put(new Windowed<>("a",
            new SessionWindow(0x7a00000000000000L - 2, 0x7a00000000000000L - 1)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions("a", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 3L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions("aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(2L, 4L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions("a", "aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions("a", "aa", 10, 0)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(2L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions(null, "aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions("a", null, 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.findSessions(null, null, 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }
    }

    @Test
    public void shouldBackwardFetchExactKeys() {
        sessionStore.close();
        sessionStore = buildSessionStore(0x7a00000000000000L, Serdes.String(), Serdes.Long());
        sessionStore.init((StateStoreContext) context, sessionStore);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);
        sessionStore.put(new Windowed<>("a",
            new SessionWindow(0x7a00000000000000L - 2, 0x7a00000000000000L - 1)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions("a", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 3L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions("aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(2L, 4L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions("a", "aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions("a", "aa", 10, 0)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(2L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions(null, "aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions("a", null, 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                 sessionStore.backwardFindSessions(null, null, 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }
    }

    @Test
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final SessionStore<Bytes, String> sessionStore =
            buildSessionStore(RETENTION_PERIOD, Serdes.Bytes(), Serdes.String());

        sessionStore.init((StateStoreContext) context, sessionStore);

        final Bytes key1 = Bytes.wrap(new byte[] {0});
        final Bytes key2 = Bytes.wrap(new byte[] {0, 0});
        final Bytes key3 = Bytes.wrap(new byte[] {0, 0, 0});

        sessionStore.put(new Windowed<>(key1, new SessionWindow(1, 100)), "1");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(2, 100)), "2");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(3, 100)), "3");
        sessionStore.put(new Windowed<>(key1, new SessionWindow(4, 100)), "4");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(5, 100)), "5");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(6, 100)), "6");
        sessionStore.put(new Windowed<>(key1, new SessionWindow(7, 100)), "7");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(8, 100)), "8");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(9, 100)), "9");

        final List<String> expectedKey1 = asList("1", "4", "7");
        try (KeyValueIterator<Windowed<Bytes>, String> iterator = sessionStore.findSessions(key1, 0L, Long.MAX_VALUE)) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(expectedKey1)));
        }

        final List<String> expectedKey2 = asList("2", "5", "8");
        try (KeyValueIterator<Windowed<Bytes>, String> iterator = sessionStore.findSessions(key2, 0L, Long.MAX_VALUE)) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(expectedKey2)));
        }

        final List<String> expectedKey3 = asList("3", "6", "9");
        try (KeyValueIterator<Windowed<Bytes>, String> iterator = sessionStore.findSessions(key3, 0L, Long.MAX_VALUE)) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(expectedKey3)));
        }

        sessionStore.close();
    }

    @Test
    public void shouldBackwardFetchAndIterateOverExactBinaryKeys() {
        final SessionStore<Bytes, String> sessionStore =
            buildSessionStore(RETENTION_PERIOD, Serdes.Bytes(), Serdes.String());

        sessionStore.init((StateStoreContext) context, sessionStore);

        final Bytes key1 = Bytes.wrap(new byte[] {0});
        final Bytes key2 = Bytes.wrap(new byte[] {0, 0});
        final Bytes key3 = Bytes.wrap(new byte[] {0, 0, 0});

        sessionStore.put(new Windowed<>(key1, new SessionWindow(1, 100)), "1");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(2, 100)), "2");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(3, 100)), "3");
        sessionStore.put(new Windowed<>(key1, new SessionWindow(4, 100)), "4");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(5, 100)), "5");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(6, 100)), "6");
        sessionStore.put(new Windowed<>(key1, new SessionWindow(7, 100)), "7");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(8, 100)), "8");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(9, 100)), "9");


        final List<String> expectedKey1 = asList("7", "4", "1");
        try (KeyValueIterator<Windowed<Bytes>, String> iterator = sessionStore.backwardFindSessions(key1, 0L, Long.MAX_VALUE)) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(expectedKey1)));
        }

        final List<String> expectedKey2 = asList("8", "5", "2");
        try (KeyValueIterator<Windowed<Bytes>, String> iterator = sessionStore.backwardFindSessions(key2, 0L, Long.MAX_VALUE)) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(expectedKey2)));
        }

        final List<String> expectedKey3 = asList("9", "6", "3");
        try (KeyValueIterator<Windowed<Bytes>, String> iterator = sessionStore.backwardFindSessions(key3, 0L, Long.MAX_VALUE)) {
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(expectedKey3)));
        }

        sessionStore.close();
    }

    @Test
    public void testIteratorPeek() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions("a", 0L, 20)) {

            assertEquals(iterator.peekNextKey(), new Windowed<>("a", new SessionWindow(0L, 0L)));
            assertEquals(iterator.peekNextKey(), iterator.next().key);
            assertEquals(iterator.peekNextKey(), iterator.next().key);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testIteratorPeekBackward() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.backwardFindSessions("a", 0L, 20)) {

            assertEquals(iterator.peekNextKey(), new Windowed<>("a", new SessionWindow(10L, 20L)));
            assertEquals(iterator.peekNextKey(), iterator.next().key);
            assertEquals(iterator.peekNextKey(), iterator.next().key);
            assertFalse(iterator.hasNext());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRestore() {
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }

        sessionStore.close();

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(Collections.emptyList(), toList(values));
        }


        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        context.restore(sessionStore.name(), changeLog);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("b", new SessionWindow(10, 50)), 2L);
        sessionStore.put(new Windowed<>("c", new SessionWindow(100, 500)), 3L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.fetch("a")) {
            assertTrue(iterator.hasNext());
            sessionStore.close();

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1)), 0L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(2, 3)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(4, 5)), 2L);
        sessionStore.put(new Windowed<>("aaa", new SessionWindow(6, 7)), 3L);

        try (final KeyValueIterator<Windowed<String>, Long> singleKeyIterator = sessionStore.findSessions("aa", 0L, 10L);
             final KeyValueIterator<Windowed<String>, Long> rangeIterator = sessionStore.findSessions("aa", "aa", 0L, 10L)) {

            assertEquals(singleKeyIterator.next(), rangeIterator.next());
            assertEquals(singleKeyIterator.next(), rangeIterator.next());
            assertFalse(singleKeyIterator.hasNext());
            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void shouldMeasureExpiredRecords() {
        final Properties streamsConfig = StreamsTestUtils.getStreamsConfig();
        final SessionStore<String, Long> sessionStore = buildSessionStore(RETENTION_PERIOD, Serdes.String(), Serdes.Long());
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            new StreamsConfig(streamsConfig),
            recordCollector
        );
        final Time time = new SystemTime();
        context.setTime(1L);
        context.setSystemTimeMs(time.milliseconds());
        sessionStore.init((StateStoreContext) context, sessionStore);

        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
        // Note that rocksdb will only expire segments at a time (where segment interval = 60,000 for this retention period)
        sessionStore.put(new Windowed<>("initial record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

        // Try inserting a record with timestamp 0 -- should be dropped
        sessionStore.put(new Windowed<>("late record", new SessionWindow(0, 0)), 0L);
        sessionStore.put(new Windowed<>("another on-time record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        final String threadId = Thread.currentThread().getName();
        final Metric dropTotal;
        final Metric dropRate;
        dropTotal = metrics.get(new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        ));

        dropRate = metrics.get(new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        ));
        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());

        sessionStore.close();
    }

    @Test
    public void shouldNotThrowExceptionRemovingNonexistentKey() {
        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1)));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnFindSessionsNullKey() {
        assertThrows(NullPointerException.class, () -> sessionStore.findSessions(null, 1L, 2L));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnFetchNullKey() {
        assertThrows(NullPointerException.class, () -> sessionStore.fetch(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnRemoveNullKey() {
        assertThrows(NullPointerException.class, () -> sessionStore.remove(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        assertThrows(NullPointerException.class, () -> sessionStore.put(null, 1L));
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        final String keyFrom = Serdes.String().deserializer()
            .deserialize("", Serdes.Integer().serializer().serialize("", -1));
        final String keyTo = Serdes.String().deserializer()
            .deserialize("", Serdes.Integer().serializer().serialize("", 1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
             final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions(keyFrom, keyTo, 0L, 10L)) {
            assertFalse(iterator.hasNext());

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }

    @Test
    public void shouldRemoveExpired() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        if (getStoreType() == StoreType.InMemoryStore) {
            sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
            sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);

            // Advance stream time to expire the first record
            sessionStore.put(new Windowed<>("aa", new SessionWindow(10, RETENTION_PERIOD)), 4L);
        } else {
            sessionStore.put(new Windowed<>("aa", new SessionWindow(0, SEGMENT_INTERVAL)), 2L);
            sessionStore.put(new Windowed<>("a", new SessionWindow(10, SEGMENT_INTERVAL)), 3L);

            // Advance stream time to expire the first record
            sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 2 * SEGMENT_INTERVAL)), 4L);
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("a", "b", 0L, Long.MAX_VALUE)
        ) {
            if (getStoreType() == StoreType.InMemoryStore) {
                assertEquals(valuesToSet(iterator), new HashSet<>(Arrays.asList(2L, 3L, 4L)));
            } else {
                // The 2 records with values 2L and 3L are considered expired as
                // their end times < observed stream time - retentionPeriod + 1.
                Assertions.assertEquals(valuesToSet(iterator), new HashSet<>(Collections.singletonList(4L)));
            }
        }
    }

    @Test
    public void shouldMatchPositionAfterPut() {
        final MeteredSessionStore<String, Long> meteredSessionStore = (MeteredSessionStore<String, Long>) sessionStore;
        final ChangeLoggingSessionBytesStore changeLoggingSessionBytesStore = (ChangeLoggingSessionBytesStore) meteredSessionStore.wrapped();
        final SessionStore wrapped = (SessionStore) changeLoggingSessionBytesStore.wrapped();

        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        sessionStore.put(new Windowed<String>("a", new SessionWindow(0, 0)), 1L);
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        sessionStore.put(new Windowed<String>("aa", new SessionWindow(0, 10)), 2L);
        context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
        sessionStore.put(new Windowed<String>("a", new SessionWindow(10, 20)), 3L);

        final Position expected = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 3L)))));
        final Position actual = sessionStore.getPosition();
        assertThat(expected, is(actual));
    }

    @Test
    public void shouldNotFetchExpiredSessions() {
        final long systemTime = Time.SYSTEM.milliseconds();
        sessionStore.put(new Windowed<>("p", new SessionWindow(systemTime - 3 * RETENTION_PERIOD, systemTime - 2 * RETENTION_PERIOD)), 1L);
        sessionStore.put(new Windowed<>("q", new SessionWindow(systemTime - 2 * RETENTION_PERIOD, systemTime - RETENTION_PERIOD)), 4L);
        sessionStore.put(new Windowed<>("r", new SessionWindow(systemTime - RETENTION_PERIOD, systemTime - RETENTION_PERIOD / 2)), 3L);
        sessionStore.put(new Windowed<>("p", new SessionWindow(systemTime - RETENTION_PERIOD, systemTime - RETENTION_PERIOD / 2)), 2L);
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                     sessionStore.findSessions("p", systemTime - 2 * RETENTION_PERIOD, systemTime - RETENTION_PERIOD)
        ) {
            Assertions.assertEquals(mkSet(2L), valuesToSet(iterator));
        }
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                     sessionStore.backwardFindSessions("p", systemTime - 5 * RETENTION_PERIOD, systemTime - 4 * RETENTION_PERIOD)
        ) {
            Assertions.assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                     sessionStore.findSessions("p", "r", systemTime - 5 * RETENTION_PERIOD, systemTime - 4 * RETENTION_PERIOD)
        ) {
            Assertions.assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                     sessionStore.findSessions("p", "r", systemTime - RETENTION_PERIOD, systemTime - RETENTION_PERIOD / 2)
        ) {
            Assertions.assertEquals(valuesToSet(iterator), mkSet(2L, 3L, 4L));
        }
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                     sessionStore.findSessions("p", "r", systemTime - 2 * RETENTION_PERIOD, systemTime - RETENTION_PERIOD)
        ) {
            Assertions.assertEquals(valuesToSet(iterator), mkSet(2L, 3L, 4L));
        }
        try (final KeyValueIterator<Windowed<String>, Long> iterator =
                     sessionStore.backwardFindSessions("p", "r", systemTime - 2 * RETENTION_PERIOD, systemTime - RETENTION_PERIOD)
        ) {
            Assertions.assertEquals(valuesToSet(iterator), mkSet(2L, 3L, 4L));
        }
    }
}
