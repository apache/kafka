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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


public class CachingWindowStoreTest {

    private static final int MAX_CACHE_SIZE_BYTES = 150;
    private static final Long WINDOW_SIZE = 10000L;
    private RocksDBWindowStore<Bytes, byte[]> underlying;
    private CachingWindowStore<String, String> cachingStore;
    private CachingKeyValueStoreTest.CacheFlushListenerStub<Windowed<String>> cacheListener;
    private ThreadCache cache;
    private String topic;
    private static final Long DEFAULT_TIMESTAMP = 10L;

    @Before
    public void setUp() throws Exception {
        underlying = new RocksDBWindowStore<>("test", 30000, 3, false, Serdes.Bytes(), Serdes.ByteArray());
        cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>();
        cachingStore = new CachingWindowStore<>(underlying,
                                                Serdes.String(),
                                                Serdes.String(),
                                                WINDOW_SIZE);
        cachingStore.setFlushListener(cacheListener);
        cache = new ThreadCache(MAX_CACHE_SIZE_BYTES);
        topic = "topic";
        final MockProcessorContext context = new MockProcessorContext(null, TestUtils.tempDirectory(), null, null, (RecordCollector) null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic));
        cachingStore.init(context, cachingStore);
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
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws Exception {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        final WindowStoreIterator<byte[]> iter = underlying.fetch(Bytes.wrap("0".getBytes()), DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
        final KeyValue<Long, byte[]> next = iter.next();
        assertEquals(DEFAULT_TIMESTAMP, next.key);
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
        underlying.put(Bytes.wrap("1".getBytes()), "a".getBytes());
        cachingStore.put("1", "b", DEFAULT_TIMESTAMP + WINDOW_SIZE);
        final WindowStoreIterator<String> fetch = cachingStore.fetch("1", DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE);
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP, "a"), fetch.next());
        assertEquals(KeyValue.pair(DEFAULT_TIMESTAMP + WINDOW_SIZE, "b"), fetch.next());
        assertFalse(fetch.hasNext());
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