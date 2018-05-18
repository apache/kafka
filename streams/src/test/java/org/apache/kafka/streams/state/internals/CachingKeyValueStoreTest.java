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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CachingKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private final int maxCacheSizeBytes = 150;
    private InternalMockProcessorContext context;
    private CachingKeyValueStore<String, String> store;
    private InMemoryKeyValueStore<Bytes, byte[]> underlyingStore;
    private ThreadCache cache;
    private CacheFlushListenerStub<String, String> cacheFlushListener;
    private String topic;

    @Before
    public void setUp() {
        final String storeName = "store";
        underlyingStore = new InMemoryKeyValueStore<>(storeName, Serdes.Bytes(), Serdes.ByteArray());
        cacheFlushListener = new CacheFlushListenerStub<>();
        store = new CachingKeyValueStore<>(underlyingStore, Serdes.String(), Serdes.String());
        store.setFlushListener(cacheFlushListener, false);
        cache = new ThreadCache(new LogContext("testCache "), maxCacheSizeBytes, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext(null, null, null, (RecordCollector) null, cache);
        topic = "topic";
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, topic));
        store.init(context, null);
    }

    @After
    public void after() {
        super.after();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context) {
        final StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("cache-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde())
                .withCachingEnabled();

        final KeyValueStore<K, V> store = (KeyValueStore<K, V>) storeBuilder.build();
        final CacheFlushListenerStub<K, V> cacheFlushListener = new CacheFlushListenerStub<>();

        final CachedStateStore inner = (CachedStateStore) ((WrappedStateStore) store).wrappedStore();
        inner.setFlushListener(cacheFlushListener, false);
        store.init(context, store);
        return store;
    }

    @Test
    public void shouldPutGetToFromCache() {
        store.put(bytesKey("key"), bytesValue("value"));
        store.put(bytesKey("key2"), bytesValue("value2"));
        assertThat(store.get(bytesKey("key")), equalTo(bytesValue("value")));
        assertThat(store.get(bytesKey("key2")), equalTo(bytesValue("value2")));
        // nothing evicted so underlying store should be empty
        assertEquals(2, cache.size());
        assertEquals(0, underlyingStore.approximateNumEntries());
    }

    private byte[] bytesValue(final String value) {
        return value.getBytes();
    }

    private Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws IOException {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, store.approximateNumEntries());
        assertNotNull(underlyingStore.get(Bytes.wrap("0".getBytes())));
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() throws IOException {
        int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheFlushListener.forwarded.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() {
        store.setFlushListener(cacheFlushListener, true);
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        store.put(bytesKey("1"), bytesValue("b"));
        store.flush();
        assertEquals("b", cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("a", cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldNotForwardOldValuesWhenDisabled() {
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        store.put(bytesKey("1"), bytesValue("b"));
        store.flush();
        assertEquals("b", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldIterateAllStoredItems() throws IOException {
        int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> all = store.all();
        final List<Bytes> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldIterateOverRange() throws IOException {
        int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> range = store.range(bytesKey(String.valueOf(0)), bytesKey(String.valueOf(items)));
        final List<Bytes> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldDeleteItemsFromCache() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        assertFalse(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.all().hasNext());
    }

    @Test
    public void shouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.flush();
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        assertFalse(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.all().hasNext());
    }

    @Test
    public void shouldClearNamespaceCacheOnClose() {
        store.put(bytesKey("a"), bytesValue("a"));
        assertEquals(1, cache.size());
        store.close();
        assertEquals(0, cache.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToGetFromClosedCachingStore() {
        store.close();
        store.get(bytesKey("a"));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToWriteToClosedCachingStore() {
        store.close();
        store.put(bytesKey("a"), bytesValue("a"));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDoRangeQueryOnClosedCachingStore() {
        store.close();
        store.range(bytesKey("a"), bytesKey("b"));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDoAllQueryOnClosedCachingStore() {
        store.close();
        store.all();
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDoGetApproxSizeOnClosedCachingStore() {
        store.close();
        store.approximateNumEntries();
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDoPutAllClosedCachingStore() {
        store.close();
        store.putAll(Collections.singletonList(KeyValue.pair(bytesKey("a"), bytesValue("a"))));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDoPutIfAbsentClosedCachingStore() {
        store.close();
        store.putIfAbsent(bytesKey("b"), bytesValue("c"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutWithNullKey() {
        store.put(null, bytesValue("c"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey() {
        store.putIfAbsent(null, bytesValue("c"));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutAllWithNullKey() {
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<Bytes, byte[]>(null, bytesValue("a")));
        try {
            store.putAll(entries);
            fail("Should have thrown NullPointerException while putAll null key");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldPutIfAbsent() {
        store.putIfAbsent(bytesKey("b"), bytesValue("2"));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValue("2")));

        store.putIfAbsent(bytesKey("b"), bytesValue("3"));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValue("2")));
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(bytesKey("a"), bytesValue("1")));
        entries.add(new KeyValue<>(bytesKey("b"), bytesValue("2")));
        store.putAll(entries);
        assertThat(store.get(bytesKey("a")), equalTo(bytesValue("1")));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValue("2")));
    }

    @Test
    public void shouldReturnUnderlying() {
        assertTrue(store.underlying().equals(underlyingStore));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDeleteFromClosedCachingStore() {
        store.close();
        store.delete(bytesKey("key"));
    }

    private int addItemsToCache() throws IOException {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            final String kv = String.valueOf(i++);
            store.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic);
        }
        return i;
    }

    public static class CacheFlushListenerStub<K, V> implements CacheFlushListener<K, V> {
        final Map<K, Change<V>> forwarded = new HashMap<>();

        @Override
        public void apply(final K key, final V newValue, final V oldValue) {
            forwarded.put(key, new Change<>(newValue, oldValue));
        }
    }
}