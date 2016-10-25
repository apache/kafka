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
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CachingKeyValueStoreTest {

    private CachingKeyValueStore<String, String> store;
    private InMemoryKeyValueStore<Bytes, byte[]> underlyingStore;
    private ThreadCache cache;
    private int maxCacheSizeBytes;
    private CacheFlushListenerStub<String> cacheFlushListener;
    private String topic;

    @Before
    public void setUp() throws Exception {
        final String storeName = "store";
        underlyingStore = new InMemoryKeyValueStore<>(storeName);
        cacheFlushListener = new CacheFlushListenerStub<>();
        store = new CachingKeyValueStore<>(underlyingStore, Serdes.String(), Serdes.String());
        store.setFlushListener(cacheFlushListener);
        maxCacheSizeBytes = 150;
        cache = new ThreadCache(maxCacheSizeBytes);
        final MockProcessorContext context = new MockProcessorContext(null, null, null, null, (RecordCollector) null, cache);
        topic = "topic";
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, topic));
        store.init(context, null);
    }

    @Test
    public void testPutGetRange() {
        // Verify that the store reads and writes correctly ...
        store.put("0", "zero");
        store.put("1", "one");
        store.put("2", "two");
        store.put("4", "four");
        store.put("5", "five");
        assertEquals("zero", store.get("0"));
        assertEquals("one", store.get("1"));
        assertEquals("two", store.get("2"));
        assertNull(store.get("3"));
        assertEquals("four", store.get("4"));
        assertEquals("five", store.get("5"));
        store.delete("5");

        try {
            // Check range iteration ...
            try (KeyValueIterator<String, String> iter = store.range("2", "4")) {
                assertTrue("First row exists", iter.hasNext());
                assertEquals(new KeyValue<>("2", "two"), iter.next());
                assertTrue("Second row exists", iter.hasNext());
                assertEquals(new KeyValue<>("4", "four"), iter.next());
                assertFalse("No more rows", iter.hasNext());
            }

            // Check range iteration ...
            try (KeyValueIterator<String, String> iter = store.range("2", "6")) {
                assertTrue("First row exists", iter.hasNext());
                assertEquals(new KeyValue<>("2", "two"), iter.next());
                assertTrue("Second row exists", iter.hasNext());
                assertEquals(new KeyValue<>("4", "four"), iter.next());
                assertFalse("No more rows", iter.hasNext());
            }

            // Check rangeUntil() ...
            try (KeyValueIterator<String, String> iter = store.rangeUntil("2")) {
                assertTrue("First row exists", iter.hasNext());
                assertEquals(new KeyValue<>("0", "zero"), iter.next());
                assertTrue("Second row exists", iter.hasNext());
                assertEquals(new KeyValue<>("1", "one"), iter.next());
                assertTrue("Third row exists", iter.hasNext());
                assertEquals(new KeyValue<>("2", "two"), iter.next());
                assertFalse("No more rows", iter.hasNext());
            }

            // Check rangeFrom()
            try (KeyValueIterator<String, String> iter = store.rangeFrom("2")) {
                assertTrue("First row exists", iter.hasNext());
                assertEquals(new KeyValue<>("2", "two"), iter.next());
                assertTrue("Second row exists", iter.hasNext());
                assertEquals(new KeyValue<>("4", "four"), iter.next());
                assertFalse("No more rows", iter.hasNext());
            }
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldPutGetToFromCache() throws Exception {
        store.put("key", "value");
        store.put("key2", "value2");
        assertEquals("value", store.get("key"));
        assertEquals("value2", store.get("key2"));
        // nothing evicted so underlying store should be empty
        assertEquals(2, cache.size());
        assertEquals(0, underlyingStore.approximateNumEntries());
    }

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws Exception {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, store.approximateNumEntries());
        assertNotNull(underlyingStore.get(Bytes.wrap("0".getBytes())));
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() throws Exception {
        int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheFlushListener.forwarded.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() throws Exception {
        store.put("1", "a");
        store.flush();
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() throws Exception {
        store.put("1", "a");
        store.flush();
        store.put("1", "b");
        store.flush();
        assertEquals("b", cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("a", cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldIterateAllStoredItems() throws Exception {
        int items = addItemsToCache();
        final KeyValueIterator<String, String> all = store.all();
        final List<String> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldIterateOverRange() throws Exception {
        int items = addItemsToCache();
        final KeyValueIterator<String, String> range = store.range(String.valueOf(0), String.valueOf(items));
        final List<String> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldDeleteItemsFromCache() throws Exception {
        store.put("a", "a");
        store.delete("a");
        assertNull(store.get("a"));
        assertFalse(store.range("a", "b").hasNext());
        assertFalse(store.all().hasNext());
    }

    @Test
    public void shouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() throws Exception {
        store.put("a", "a");
        store.flush();
        store.delete("a");
        assertNull(store.get("a"));
        assertFalse(store.range("a", "b").hasNext());
        assertFalse(store.all().hasNext());
    }

    private int addItemsToCache() throws IOException {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            final String kv = String.valueOf(i++);
            store.put(kv, kv);
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic);
        }
        return i;
    }

    public static class CacheFlushListenerStub<K> implements CacheFlushListener<K, String> {
        public final Map<K, Change<String>> forwarded = new HashMap<>();

        @Override
        public void apply(final K key, final String newValue, final String oldValue) {
            forwarded.put(key, new Change<>(newValue, oldValue));
        }
    }
}