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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.kafka.streams.state.internals.RocksDBSessionStoreTest.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class CachingSessionStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 600;
    private final StateSerdes<String, Long> serdes =
            new StateSerdes<>("name", Serdes.String(), Serdes.Long());
    private RocksDBSegmentedBytesStore underlying;
    private CachingSessionStore<String, Long> cachingStore;
    private ThreadCache cache;
    private static final Long DEFAULT_TIMESTAMP = 10L;

    @Before
    public void setUp() throws Exception {
        underlying = new RocksDBSegmentedBytesStore("test", 60000, 3, new SessionKeySchema());
        final RocksDBSessionStore<Bytes, byte[]> sessionStore = new RocksDBSessionStore<>(underlying, Serdes.Bytes(), Serdes.ByteArray());
        cachingStore = new CachingSessionStore<>(sessionStore,
                                                 Serdes.String(),
                                                 Serdes.Long());
        cache = new ThreadCache("testCache", MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        final MockProcessorContext context = new MockProcessorContext(TestUtils.tempDirectory(), null, null, (RecordCollector) null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, "topic"));
        cachingStore.init(context, cachingStore);
    }

    @After
    public void close() {
        cachingStore.close();
    }

    @Test
    public void shouldPutFetchFromCache() throws Exception {
        cachingStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        cachingStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 1L);
        cachingStore.put(new Windowed<>("b", new SessionWindow(0, 0)), 1L);

        final KeyValueIterator<Windowed<String>, Long> a = cachingStore.findSessions("a", 0, 0);
        final KeyValueIterator<Windowed<String>, Long> b = cachingStore.findSessions("b", 0, 0);

        assertEquals(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L), a.next());
        assertEquals(KeyValue.pair(new Windowed<>("b", new SessionWindow(0, 0)), 1L), b.next());
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
        assertEquals(3, cache.size());
    }

    @Test
    public void shouldFetchAllSessionsWithSameRecordKey() throws Exception {

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));
        for (KeyValue<Windowed<String>, Long> kv : expected) {
            cachingStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        cachingStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);


        final List<KeyValue<Windowed<String>, Long>> results = toList(cachingStore.fetch("a"));
        assertEquals(expected, results);

    }

    @Test
    public void shouldFlushItemsToStoreOnEviction() throws Exception {
        final List<KeyValue<Windowed<String>, Long>> added = addSessionsUntilOverflow("a", "b", "c", "d");
        assertEquals(added.size() - 1, cache.size());
        final KeyValueIterator<Bytes, byte[]> iterator = underlying.fetch(Bytes.wrap(added.get(0).key.key().getBytes()), 0, 0);
        final KeyValue<Bytes, byte[]> next = iterator.next();
        assertEquals(added.get(0).key, SessionKeySerde.from(next.key.get(), Serdes.String().deserializer()));
        assertArrayEquals(serdes.rawValue(added.get(0).value), next.value);
    }

    @Test
    public void shouldQueryItemsInCacheAndStore() throws Exception {
        final List<KeyValue<Windowed<String>, Long>> added = addSessionsUntilOverflow("a");
        final KeyValueIterator<Windowed<String>, Long> iterator = cachingStore.findSessions("a", 0, added.size() * 10);
        final List<KeyValue<Windowed<String>, Long>> actual = toList(iterator);
        assertEquals(added, actual);
    }

    @Test
    public void shouldRemove() throws Exception {
        final Windowed<String> a = new Windowed<>("a", new SessionWindow(0, 0));
        final Windowed<String> b = new Windowed<>("b", new SessionWindow(0, 0));
        cachingStore.put(a, 2L);
        cachingStore.put(b, 2L);
        cachingStore.flush();
        cachingStore.remove(a);
        cachingStore.flush();
        final KeyValueIterator<Windowed<String>, Long> rangeIter = cachingStore.findSessions("a", 0, 0);
        assertFalse(rangeIter.hasNext());
    }

    @Test
    public void shouldFetchCorrectlyAcrossSegments() throws Exception {
        final Windowed<String> a1 = new Windowed<>("a", new SessionWindow(0, 0));
        final Windowed<String> a2 = new Windowed<>("a", new SessionWindow(Segments.MIN_SEGMENT_INTERVAL, Segments.MIN_SEGMENT_INTERVAL));
        final Windowed<String> a3 = new Windowed<>("a", new SessionWindow(Segments.MIN_SEGMENT_INTERVAL * 2, Segments.MIN_SEGMENT_INTERVAL * 2));
        cachingStore.put(a1, 1L);
        cachingStore.put(a2, 2L);
        cachingStore.put(a3, 3L);
        cachingStore.flush();
        final KeyValueIterator<Windowed<String>, Long> results = cachingStore.findSessions("a", 0, Segments.MIN_SEGMENT_INTERVAL * 2);
        assertEquals(a1, results.next().key);
        assertEquals(a2, results.next().key);
        assertEquals(a3, results.next().key);
        assertFalse(results.hasNext());
    }

    @Test
    public void shouldClearNamespaceCacheOnClose() throws Exception {
        final Windowed<String> a1 = new Windowed<>("a", new SessionWindow(0, 0));
        cachingStore.put(a1, 1L);
        assertEquals(1, cache.size());
        cachingStore.close();
        assertEquals(0, cache.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.fetch("a");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFindMergeSessionFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.findSessions("a", 0, Long.MAX_VALUE);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToRemoveFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.remove(new Windowed<>("a", new SessionWindow(0, 0)));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToPutIntoClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
    }

    private List<KeyValue<Windowed<String>, Long>> addSessionsUntilOverflow(final String...sessionIds) {
        final Random random = new Random();
        final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
        while (cache.size() == results.size()) {
            final String sessionId = sessionIds[random.nextInt(sessionIds.length)];
            addSingleSession(sessionId, results);
        }
        return results;
    }

    private void addSingleSession(final String sessionId, final List<KeyValue<Windowed<String>, Long>> allSessions) {
        final int timestamp = allSessions.size() * 10;
        final Windowed<String> key = new Windowed<>(sessionId, new SessionWindow(timestamp, timestamp));
        final Long value = 1L;
        cachingStore.put(key, value);
        allSessions.add(KeyValue.pair(key, value));
    }


}
