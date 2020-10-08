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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.toSet;
import static org.apache.kafka.test.StreamsTestUtils.valuesToSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public abstract class AbstractSessionBytesStoreTest {

    static final long SEGMENT_INTERVAL = 60_000L;
    static final long RETENTION_PERIOD = 10_000L;

    SessionStore<String, Long> sessionStore;

    private MockRecordCollector recordCollector;

    private InternalMockProcessorContext context;

    abstract <K, V> SessionStore<K, V> buildSessionStore(final long retentionPeriod,
                                                         final Serde<K> keySerde,
                                                         final Serde<V> valueSerde);

    abstract String getMetricsScope();

    @Before
    public void setUp() {
        sessionStore = buildSessionStore(RETENTION_PERIOD, Serdes.String(), Serdes.Long());
        recordCollector = new MockRecordCollector();
        context = new InternalMockProcessorContext(
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
            assertEquals(new HashSet<>(expected), toSet(values));
        }

        final List<KeyValue<Windowed<String>, Long>> expected2 =
            Collections.singletonList(KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values2 = sessionStore.findSessions(key, 400L, 600L)
        ) {
            assertEquals(new HashSet<>(expected2), toSet(values2));
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

        final List<KeyValue<Windowed<String>, Long>> expected =
            asList(KeyValue.pair(a1, 1L), KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFindSessions(key, 0, 1000L)) {
            assertEquals(new HashSet<>(expected), toSet(values));
        }

        final List<KeyValue<Windowed<String>, Long>> expected2 =
            Collections.singletonList(KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values2 = sessionStore.backwardFindSessions(key, 400L, 600L)) {
            assertEquals(new HashSet<>(expected2), toSet(values2));
        }
    }

    @Test
    public void shouldFetchAllSessionsWithSameRecordKey() {
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(new HashSet<>(expected), toSet(values));
        }
    }

    @Test
    public void shouldBackwardFetchAllSessionsWithSameRecordKey() {
        final List<KeyValue<Windowed<String>, Long>> expected = asList(
            KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L)
        );

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch("a")) {
            assertEquals(new HashSet<>(expected), toSet(values));
        }
    }

    @Test
    public void shouldFetchAllSessionsWithinKeyRange() {
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>("aa", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("b", new SessionWindow(1000, 1000)), 4L),

            KeyValue.pair(new Windowed<>("aaa", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("bb", new SessionWindow(1500, 2000)), 5L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add some that shouldn't appear in the results
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("aa", "bb")) {
            assertEquals(new HashSet<>(expected), toSet(values));
        }
    }

    @Test
    public void shouldBackwardFetchAllSessionsWithinKeyRange() {
        final List<KeyValue<Windowed<String>, Long>> expected = asList(
            KeyValue.pair(new Windowed<>("aa", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("b", new SessionWindow(1000, 1000)), 4L),

            KeyValue.pair(new Windowed<>("aaa", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("bb", new SessionWindow(1500, 2000)), 5L)
        );

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add some that shouldn't appear in the results
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.backwardFetch("aa", "bb")) {
            assertEquals(new HashSet<>(expected), toSet(values));
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
            assertEquals(new HashSet<>(expected), toSet(results));
        }
    }

    @Test
    public void shouldBackwardFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L)
        );

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.backwardFindSessions(key, -1, 1000L)) {
            assertEquals(new HashSet<>(expected), toSet(results));
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
            assertEquals(new HashSet<>(expected), toSet(results));
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
            asList(KeyValue.pair(session2, 2L), KeyValue.pair(session3, 3L));

        try (final KeyValueIterator<Windowed<String>, Long> results = sessionStore.backwardFindSessions("a", 150, 300)) {
            assertEquals(new HashSet<>(expected), toSet(results));
        }
    }

    @Test
    public void shouldFetchExactKeys() {
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
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(Collections.singletonList(2L))));
        }
    }

    @Test
    public void shouldBackwardFetchExactKeys() {
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
            assertThat(valuesToSet(iterator), equalTo(new HashSet<>(Collections.singletonList(2L))));
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

        final Set<String> expectedKey1 = new HashSet<>(asList("1", "4", "7"));
        assertThat(valuesToSet(sessionStore.findSessions(key1, 0L, Long.MAX_VALUE)), equalTo(expectedKey1));
        final Set<String> expectedKey2 = new HashSet<>(asList("2", "5", "8"));
        assertThat(valuesToSet(sessionStore.findSessions(key2, 0L, Long.MAX_VALUE)), equalTo(expectedKey2));
        final Set<String> expectedKey3 = new HashSet<>(asList("3", "6", "9"));
        assertThat(valuesToSet(sessionStore.findSessions(key3, 0L, Long.MAX_VALUE)), equalTo(expectedKey3));
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

        final Set<String> expectedKey1 = new HashSet<>(asList("1", "4", "7"));
        assertThat(valuesToSet(sessionStore.backwardFindSessions(key1, 0L, Long.MAX_VALUE)), equalTo(expectedKey1));
        final Set<String> expectedKey2 = new HashSet<>(asList("2", "5", "8"));
        assertThat(valuesToSet(sessionStore.backwardFindSessions(key2, 0L, Long.MAX_VALUE)), equalTo(expectedKey2));
        final Set<String> expectedKey3 = new HashSet<>(asList("3", "6", "9"));
        assertThat(valuesToSet(sessionStore.backwardFindSessions(key3, 0L, Long.MAX_VALUE)), equalTo(expectedKey3));
    }

    @Test
    public void testIteratorPeek() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);

        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions("a", 0L, 20);

        assertEquals(iterator.peekNextKey(), new Windowed<>("a", new SessionWindow(0L, 0L)));
        assertEquals(iterator.peekNextKey(), iterator.next().key);
        assertEquals(iterator.peekNextKey(), iterator.next().key);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIteratorPeekBackward() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);

        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.backwardFindSessions("a", 0L, 20);

        assertEquals(iterator.peekNextKey(), new Windowed<>("a", new SessionWindow(10L, 20L)));
        assertEquals(iterator.peekNextKey(), iterator.next().key);
        assertEquals(iterator.peekNextKey(), iterator.next().key);
        assertFalse(iterator.hasNext());
    }

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
            assertEquals(new HashSet<>(expected), toSet(values));
        }

        sessionStore.close();

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(Collections.emptySet(), toSet(values));
        }


        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        context.restore(sessionStore.name(), changeLog);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(new HashSet<>(expected), toSet(values));
        }
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("b", new SessionWindow(10, 50)), 2L);
        sessionStore.put(new Windowed<>("c", new SessionWindow(100, 500)), 3L);

        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.fetch("a");
        assertTrue(iterator.hasNext());
        sessionStore.close();

        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1)), 0L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(2, 3)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(4, 5)), 2L);
        sessionStore.put(new Windowed<>("aaa", new SessionWindow(6, 7)), 3L);

        final KeyValueIterator<Windowed<String>, Long> singleKeyIterator = sessionStore.findSessions("aa", 0L, 10L);
        final KeyValueIterator<Windowed<String>, Long> rangeIterator = sessionStore.findSessions("aa", "aa", 0L, 10L);

        assertEquals(singleKeyIterator.next(), rangeIterator.next());
        assertEquals(singleKeyIterator.next(), rangeIterator.next());
        assertFalse(singleKeyIterator.hasNext());
        assertFalse(rangeIterator.hasNext());
    }

    @Test
    public void shouldLogAndMeasureExpiredRecordsWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeasureExpiredRecords(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldLogAndMeasureExpiredRecordsWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeasureExpiredRecords(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldLogAndMeasureExpiredRecords(final String builtInMetricsVersion) {
        final Properties streamsConfig = StreamsTestUtils.getStreamsConfig();
        streamsConfig.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
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

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
            // Note that rocksdb will only expire segments at a time (where segment interval = 60,000 for this retention period)
            sessionStore.put(new Windowed<>("initial record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

            // Try inserting a record with timestamp 0 -- should be dropped
            sessionStore.put(new Windowed<>("late record", new SessionWindow(0, 0)), 0L);
            sessionStore.put(new Windowed<>("another on-time record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

            final List<String> messages = appender.getMessages();
            assertThat(messages, hasItem("Skipping record for expired segment."));
        }

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        final String metricScope = getMetricsScope();
        final String threadId = Thread.currentThread().getName();
        final Metric dropTotal;
        final Metric dropRate;
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            dropTotal = metrics.get(new MetricName(
                "expired-window-record-drop-total",
                "stream-" + metricScope + "-metrics",
                "The total number of dropped records due to an expired window",
                mkMap(
                    mkEntry("client-id", threadId),
                    mkEntry("task-id", "0_0"),
                    mkEntry(metricScope + "-state-id", sessionStore.name())
                )
            ));

            dropRate = metrics.get(new MetricName(
                "expired-window-record-drop-rate",
                "stream-" + metricScope + "-metrics",
                "The average number of dropped records due to an expired window per second",
                mkMap(
                    mkEntry("client-id", threadId),
                    mkEntry("task-id", "0_0"),
                    mkEntry(metricScope + "-state-id", sessionStore.name())
                )
            ));
        } else {
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
        }
        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
    }

    @Test
    public void shouldNotThrowExceptionRemovingNonexistentKey() {
        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFindSessionsNullKey() {
        sessionStore.findSessions(null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFindSessionsNullFromKey() {
        sessionStore.findSessions(null, "anyKeyTo", 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFindSessionsNullToKey() {
        sessionStore.findSessions("anyKeyFrom", null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFetchNullFromKey() {
        sessionStore.fetch(null, "anyToKey");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFetchNullToKey() {
        sessionStore.fetch("anyFromKey", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFetchNullKey() {
        sessionStore.fetch(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRemoveNullKey() {
        sessionStore.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        sessionStore.put(null, 1L);
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        final String keyFrom = Serdes.String().deserializer()
            .deserialize("", Serdes.Integer().serializer().serialize("", -1));
        final String keyTo = Serdes.String().deserializer()
            .deserialize("", Serdes.Integer().serializer().serialize("", 1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions(keyFrom, keyTo, 0L, 10L);
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
}
