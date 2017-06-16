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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


public class CachingWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 150;
    private static final long DEFAULT_TIMESTAMP = 10L;
    private static final Long WINDOW_SIZE = 10000L;
    private MockProcessorContext context;
    private RocksDBSegmentedBytesStore underlying;
    private CachingWindowStore<String, String> cachingStore;
    private CachingKeyValueStoreTest.CacheFlushListenerStub<Windowed<String>, String> cacheListener;
    private ThreadCache cache;
    private String topic;
    private WindowKeySchema keySchema;

    @Before
    public void setUp() throws Exception {
        keySchema = new WindowKeySchema();
        final int retention = 30000;
        final int numSegments = 3;
        underlying = new RocksDBSegmentedBytesStore("test", retention, numSegments, keySchema);
        final RocksDBWindowStore<Bytes, byte[]> windowStore = new RocksDBWindowStore<>(underlying, Serdes.Bytes(), Serdes.ByteArray(), false, WINDOW_SIZE);
        cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>();
        cachingStore = new CachingWindowStore<>(windowStore,
                                                Serdes.String(),
                                                Serdes.String(),
                                                WINDOW_SIZE,
                                                Segments.segmentInterval(retention, numSegments));
        cachingStore.setFlushListener(cacheListener);
        cache = new ThreadCache("testCache", MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        topic = "topic";
        context = new MockProcessorContext(TestUtils.tempDirectory(), null, null, (RecordCollector) null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic));
        cachingStore.init(context, cachingStore);
    }

    @After
    public void closeStore() {
        context.close();
        cachingStore.close();
    }

    @Test
    public void shouldPutFetchFromCache() throws Exception {
        cachingStore.put("a", "a");
        cachingStore.put("b", "b");

        final WindowStoreIterator<String> a = cachingStore.fetch("a", 10, 10);
        final WindowStoreIterator<String> b = cachingStore.fetch("b", 10, 10);
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP, "a"), a.next());
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP, "b"), b.next());
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
        assertEquals(2, cache.size());
    }

    @Test
    public void shouldPutFetchRangeFromCache() throws Exception {
        cachingStore.put("a", "a");
        cachingStore.put("b", "b");

        final KeyValueIterator<Windowed<String>, String> iterator = cachingStore.fetch("a", "b", 10, 10);
        assertEquals(KeyValue.pair(new Windowed<>("a", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "a"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("b", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "b"), iterator.next());
        assertFalse(iterator.hasNext());
        assertEquals(2, cache.size());
    }


    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws Exception {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        final KeyValueIterator<Bytes, byte[]> iter = underlying.fetch(Bytes.wrap("0".getBytes()), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.key));
        assertArrayEquals("0".getBytes(), next.value);
        assertFalse(iter.hasNext());
        assertEquals(added - 1, cache.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() throws Exception {
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put("1", "a");
        cachingStore.flush();
        assertEquals("a", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() throws Exception {
        final Windowed<String> windowedKey = new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put("1", "a");
        cachingStore.flush();
        cachingStore.put("1", "b");
        cachingStore.flush();
        assertEquals("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertEquals("a", cacheListener.forwarded.get(windowedKey).oldValue);
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() throws Exception {
        int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheListener.forwarded.size());
    }

    @Test
    public void shouldTakeValueFromCacheIfSameTimestampFlushedToRocks() throws Exception {
        cachingStore.put("1", "a", DEFAULT_TIMESTAMP);
        cachingStore.flush();
        cachingStore.put("1", "b", DEFAULT_TIMESTAMP);

        final WindowStoreIterator<String> fetch = cachingStore.fetch("1", DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP, "b"), fetch.next());
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateAcrossWindows() throws Exception {
        cachingStore.put("1", "a", DEFAULT_TIMESTAMP);
        cachingStore.put("1", "b", DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final WindowStoreIterator<String> fetch = cachingStore.fetch("1", DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP, "a"), fetch.next());
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP + WINDOW_SIZE, "b"), fetch.next());
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStore() throws Exception {
        final Bytes key = Bytes.wrap("1" .getBytes());
        underlying.put(WindowStoreUtils.toBinaryKey(key, DEFAULT_TIMESTAMP, 0, WindowStoreUtils.getInnerStateSerde("app-id")), "a".getBytes());
        cachingStore.put("1", "b", DEFAULT_TIMESTAMP + WINDOW_SIZE);
        final WindowStoreIterator<String> fetch = cachingStore.fetch("1", DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP, "a"), fetch.next());
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP + WINDOW_SIZE, "b"), fetch.next());
        assertFalse(fetch.hasNext());
    }

    @Test
    public void shouldIterateCacheAndStoreKeyRange() throws Exception {
        final Bytes key = Bytes.wrap("1" .getBytes());
        underlying.put(WindowStoreUtils.toBinaryKey(key, DEFAULT_TIMESTAMP, 0, WindowStoreUtils.getInnerStateSerde("app-id")), "a".getBytes());
        cachingStore.put("1", "b", DEFAULT_TIMESTAMP + WINDOW_SIZE);

        final KeyValueIterator<Windowed<String>, String> fetchRange =
            cachingStore.fetch("1", "2", DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        assertEquals(KeyValue.pair(new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)), "a"), fetchRange.next());
        assertEquals(KeyValue.pair(new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)), "b"), fetchRange.next());
        assertFalse(fetchRange.hasNext());
    }

    @Test
    public void shouldClearNamespaceCacheOnClose() throws Exception {
        cachingStore.put("a", "a");
        assertEquals(1, cache.size());
        cachingStore.close();
        assertEquals(0, cache.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.fetch("a", 0, 10);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.fetch("a", "b", 0, 10);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToWriteToClosedCachingStore() throws Exception {
        cachingStore.close();
        cachingStore.put("a", "a");
    }

    @Test
    public void shouldFetchAndIterateOverExactKeys() throws Exception {
        cachingStore.put("a", "0001", 0);
        cachingStore.put("aa", "0002", 0);
        cachingStore.put("a", "0003", 1);
        cachingStore.put("aa", "0004", 1);
        cachingStore.put("a", "0005", 60000);

        final List<KeyValue<Long, String>> expected = Utils.mkList(KeyValue.pair(0L, "0001"), KeyValue.pair(1L, "0003"), KeyValue.pair(60000L, "0005"));
        assertThat(toList(cachingStore.fetch("a", 0, Long.MAX_VALUE)), equalTo(expected));
    }

    @Test
    public void shouldFetchAndIterateOverKeyRange() throws Exception {
        cachingStore.put("a", "0001", 0);
        cachingStore.put("aa", "0002", 0);
        cachingStore.put("a", "0003", 1);
        cachingStore.put("aa", "0004", 1);
        cachingStore.put("a", "0005", 60000);

        assertThat(
            toList(cachingStore.fetch("a", "a", 0, Long.MAX_VALUE)),
            equalTo(Utils.mkList(windowedPair("a", "0001", 0), windowedPair("a", "0003", 1), windowedPair("a", "0005", 60000L)))
        );

        assertThat(
            toList(cachingStore.fetch("aa", "aa", 0, Long.MAX_VALUE)),
            equalTo(Utils.mkList(windowedPair("aa", "0002", 0), windowedPair("aa", "0004", 1)))
        );

        assertThat(
            toList(cachingStore.fetch("a", "aa", 0, Long.MAX_VALUE)),
            equalTo(Utils.mkList(windowedPair("a", "0001", 0), windowedPair("a", "0003", 1), windowedPair("aa", "0002", 0), windowedPair("aa", "0004", 1), windowedPair("a", "0005", 60000L)))
        );
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() throws Exception {
        cachingStore.put(null, "anyValue");
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() throws Exception {
        cachingStore.put("a", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnFetchNullKey() throws Exception {
        cachingStore.fetch(null, 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() throws Exception {
        cachingStore.fetch(null, "anyTo", 1L, 2L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() throws Exception {
        cachingStore.fetch("anyFrom", null, 1L, 2L);
    }

    private static <K, V> KeyValue<Windowed<K>, V> windowedPair(K key, V value, long timestamp) {
        return KeyValue.pair(new Windowed<>(key, new TimeWindow(timestamp, timestamp + WINDOW_SIZE)), value);
    }

    private int addItemsToCache() throws IOException {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < MAX_CACHE_SIZE_BYTES) {
            final String kv = String.valueOf(i++);
            cachingStore.put(kv, kv);
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic) +
                8 + // timestamp
                4; // sequenceNumber
        }
        return i;
    }

}
