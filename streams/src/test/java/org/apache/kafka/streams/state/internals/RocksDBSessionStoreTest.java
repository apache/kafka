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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocksDBSessionStoreTest {

    private SessionStore<String, Long> sessionStore;
    private InternalMockProcessorContext context;

    @Before
    public void before() {
        final SessionKeySchema schema = new SessionKeySchema();
        schema.init("topic");

        final RocksDBSegmentedBytesStore bytesStore =
                new RocksDBSegmentedBytesStore("session-store", 10000L, 3, schema);

        sessionStore = new RocksDBSessionStore<>(bytesStore,
                                                 Serdes.String(),
                                                 Serdes.Long());

        context = new InternalMockProcessorContext(TestUtils.tempDirectory(),
                                           Serdes.String(),
                                           Serdes.Long(),
                                           new NoOpRecordCollector(),
                                           new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
        sessionStore.init(context, sessionStore);
    }

    @After
    public void close() {
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

        final List<KeyValue<Windowed<String>, Long>> expected
                = Arrays.asList(KeyValue.pair(a1, 1L), KeyValue.pair(a2, 2L));

        final KeyValueIterator<Windowed<String>, Long> values = sessionStore.findSessions(key, 0, 1000L);
        assertEquals(expected, toList(values));
    }

    @Test
    public void shouldFetchAllSessionsWithSameRecordKey() {

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));
        for (KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        final List<KeyValue<Windowed<String>, Long>> results = toList(sessionStore.fetch("a"));
        assertEquals(expected, results);

    }


    @Test
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);
        final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions(key, -1, 1000L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
                KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
                KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));
        assertEquals(expected, toList(results));
    }

    @Test
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));
        assertFalse(sessionStore.findSessions("a", 0, 1000L).hasNext());

        assertTrue(sessionStore.findSessions("a", 1500, 2500).hasNext());
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
        final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 150, 300);
        assertEquals(session2, results.next().key);
        assertEquals(session3, results.next().key);
        assertFalse(results.hasNext());
    }

    @Test
    public void shouldFetchExactKeys() {
        final RocksDBSegmentedBytesStore bytesStore =
                new RocksDBSegmentedBytesStore("session-store", 0x7a00000000000000L, 2, new SessionKeySchema());

        sessionStore = new RocksDBSessionStore<>(bytesStore,
                                                 Serdes.String(),
                                                 Serdes.Long());

        sessionStore.init(context, sessionStore);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(0x7a00000000000000L - 2, 0x7a00000000000000L - 1)), 5L);

        KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.findSessions("a", 0, Long.MAX_VALUE);
        List<Long> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next().value);
        }

        assertThat(results, equalTo(Arrays.asList(1L, 3L, 5L)));


        iterator = sessionStore.findSessions("aa", 0, Long.MAX_VALUE);
        results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next().value);
        }

        assertThat(results, equalTo(Arrays.asList(2L, 4L)));


        final KeyValueIterator<Windowed<String>, Long> rangeIterator = sessionStore.findSessions("a", "aa", 0, Long.MAX_VALUE);
        final List<Long> rangeResults = new ArrayList<>();
        while (rangeIterator.hasNext()) {
            rangeResults.add(rangeIterator.next().value);
        }
        assertThat(rangeResults, equalTo(Arrays.asList(1L, 3L, 2L, 4L, 5L)));
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
    
    static <K, V> List<KeyValue<Windowed<K>, V>> toList(final KeyValueIterator<Windowed<K>, V> iterator) {
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }


}
