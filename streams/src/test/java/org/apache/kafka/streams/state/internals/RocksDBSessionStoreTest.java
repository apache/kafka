/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocksDBSessionStoreTest {

    private SessionStore<String, Long> sessionStore;

    @Before
    public void before() {
        final RocksDBSegmentedBytesStore bytesStore =
                new RocksDBSegmentedBytesStore("session-store", 10000L, 3, new SessionKeySchema());

        sessionStore = new RocksDBSessionStore<>(bytesStore,
                                                 Serdes.String(),
                                                 Serdes.Long());

        final MockProcessorContext context = new MockProcessorContext(TestUtils.tempDirectory(),
                                                                      Serdes.String(),
                                                                      Serdes.Long(),
                                                                      new NoOpRecordCollector(),
                                                                      new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
        sessionStore.init(context, sessionStore);
    }

    @After
    public void close() {
        sessionStore.close();
    }

    @Test
    public void shouldPutAndFindSessionsInRange() throws Exception {
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
    public void shouldFetchAllSessionsWithSameRecordKey() throws Exception {

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
    public void shouldFindValuesWithinMergingSessionWindowRange() throws Exception {
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
    public void shouldRemove() throws Exception {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));
        assertFalse(sessionStore.findSessions("a", 0, 1000L).hasNext());

        assertTrue(sessionStore.findSessions("a", 1500, 2500).hasNext());
    }

    @Test
    public void shouldFindSessionsToMerge() throws Exception {
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

    static List<KeyValue<Windowed<String>, Long>> toList(final KeyValueIterator<Windowed<String>, Long> iterator) {
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }


}
