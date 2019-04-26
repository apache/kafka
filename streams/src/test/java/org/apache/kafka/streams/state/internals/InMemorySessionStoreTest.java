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

import static java.time.Duration.ofMillis;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.apache.kafka.test.StreamsTestUtils.valuesToList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InMemorySessionStoreTest {

    private static final String STORE_NAME = "InMemorySessionStore";
    private static final long RETENTION_PERIOD = 10_000L;

    private SessionStore<String, Long> sessionStore;
    private InternalMockProcessorContext context;

    private final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();

    private final Producer<byte[], byte[]> producer = new MockProducer<>(true,
        Serdes.ByteArray().serializer(),
        Serdes.ByteArray().serializer());

    private final RecordCollector recordCollector = new RecordCollectorImpl(
        STORE_NAME,
        new LogContext(STORE_NAME),
        new DefaultProductionExceptionHandler(),
        new Metrics().sensor("skipped-records")) {

        @Override
        public <K1, V1> void send(final String topic,
            final K1 key,
            final V1 value,
            final Headers headers,
            final Integer partition,
            final Long timestamp,
            final Serializer<K1> keySerializer,
            final Serializer<V1> valueSerializer) {
            changeLog.add(new KeyValue<>(
                keySerializer.serialize(topic, headers, key),
                valueSerializer.serialize(topic, headers, value))
            );
        }
    };

    private SessionStore<String, Long> buildSessionStore(final long retentionPeriod) {
        return Stores.sessionStoreBuilder(
            Stores.inMemorySessionStore(
                STORE_NAME,
                ofMillis(retentionPeriod)),
            Serdes.String(),
            Serdes.Long()).build();
    }

    @Before
    public void before() {
        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));

        sessionStore = buildSessionStore(RETENTION_PERIOD);

        sessionStore.init(context, sessionStore);
        recordCollector.init(producer);
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

        try (final KeyValueIterator<Windowed<String>, Long> values =
            sessionStore.findSessions(key, 0, 1000L)
        ) {
            assertEquals(expected, toList(values));
        }

        final List<KeyValue<Windowed<String>, Long>> expected2 = Collections.singletonList(KeyValue.pair(a2, 2L));

        try (final KeyValueIterator<Windowed<String>, Long> values2 =
            sessionStore.findSessions(key, 400L, 600L)
        ) {
            assertEquals(expected2, toList(values2));
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
            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldFetchAllSessionsWithinKeyRange() {
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>("aa", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("aaa", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("b", new SessionWindow(1000, 1000)), 4L),
            KeyValue.pair(new Windowed<>("bb", new SessionWindow(1500, 2000)), 5L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add some that shouldn't appear in the results
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("aa", "bb")) {
            assertEquals(expected, toList(values));
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
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results =
            sessionStore.findSessions(key, -1, 1000L)) {
            assertEquals(expected, toList(results));
        }
    }

    @Test
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try (final KeyValueIterator<Windowed<String>, Long> results =
            sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results =
            sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }

    @Test
    public void shouldRemoveOnNullAggValue() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), null);

        try (final KeyValueIterator<Windowed<String>, Long> results =
            sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results =
            sessionStore.findSessions("a", 1500L, 2500L)) {
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

        try (final KeyValueIterator<Windowed<String>, Long> results =
            sessionStore.findSessions("a", 150, 300)
        ) {
            assertEquals(session2, results.next().key);
            assertEquals(session3, results.next().key);
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void shouldFetchExactKeys() {
        sessionStore = buildSessionStore(0x7a00000000000000L);
        sessionStore.init(context, sessionStore);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(0x7a00000000000000L - 2, 0x7a00000000000000L - 1)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("a", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToList(iterator), equalTo(Arrays.asList(1L, 3L, 5L)));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToList(iterator), equalTo(Arrays.asList(2L, 4L)));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("a", "aa", 0, Long.MAX_VALUE)
        ) {
            assertThat(valuesToList(iterator), equalTo(Arrays.asList(1L, 2L, 3L, 4L, 5L)));
        }

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("a", "aa", 10, 0)
        ) {
            assertThat(valuesToList(iterator), equalTo(Collections.singletonList(2L)));
        }
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
    public void shouldRemoveExpired() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);

        // Advance stream time to expire the first record
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, RETENTION_PERIOD)), 4L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("a", "b", 0L, Long.MAX_VALUE)
        ) {
            assertThat(valuesToList(iterator), equalTo(Arrays.asList(2L, 3L, 4L)));
        }
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
            assertEquals(expected, toList(values));
        }

        sessionStore.close();

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(Collections.emptyList(), toList(values));
        }

        context.restore(STORE_NAME, changeLog);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }

    @Test
    public void shouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1)), 0L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(2, 3)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(4, 5)), 2L);
        sessionStore.put(new Windowed<>("aaa", new SessionWindow(6, 7)), 3L);

        final KeyValueIterator<Windowed<String>, Long> singleKeyIterator = sessionStore.findSessions("aa", 0L, 10L);
        final KeyValueIterator<Windowed<String>, Long> keyRangeIterator = sessionStore.findSessions("aa", "aa", 0L, 10L);

        assertEquals(singleKeyIterator.next(), keyRangeIterator.next());
        assertEquals(singleKeyIterator.next(), keyRangeIterator.next());
        assertFalse(singleKeyIterator.hasNext());
        assertFalse(keyRangeIterator.hasNext());
    }

    @Test
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(InMemorySessionStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();


        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
        sessionStore.put(new Windowed<>("initial record", new SessionWindow(0, RETENTION_PERIOD)), 0L);

        // Try inserting a record with timestamp 0 -- should be dropped
        sessionStore.put(new Windowed<>("late record", new SessionWindow(0, 0)), 0L);
        sessionStore.put(new Windowed<>("another on-time record", new SessionWindow(0, RETENTION_PERIOD)), 0L);

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-in-memory-session-state-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("in-memory-session-state-id", STORE_NAME)
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-in-memory-session-state-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("in-memory-session-state-id", STORE_NAME)
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
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
        LogCaptureAppender.setClassLoggerToDebug(InMemorySessionStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final String keyFrom = Serdes.String().deserializer().deserialize("", Serdes.Integer().serializer().serialize("", -1));
        final String keyTo = Serdes.String().deserializer().deserialize("", Serdes.Integer().serializer().serialize("", 1));

        final KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions(keyFrom, keyTo, 0L, 10L);
        assertFalse(iterator.hasNext());

        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
            + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
            + "Note that the built-in numerical serdes do not follow this for negative numbers"));
    }
}