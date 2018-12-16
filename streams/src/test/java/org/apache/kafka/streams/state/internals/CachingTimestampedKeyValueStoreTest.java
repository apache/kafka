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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CachingTimestampedKeyValueStoreTest {

    private final int maxCacheSizeBytes = 150;
    private ThreadCache cache;
    private CachingTimestampedKeyValueStore<String, String> store;
    private InMemoryKeyValueStore<Bytes, byte[]> underlyingStore;
    private CacheFlushListenerStub<String, String> cacheFlushListener;
    private ProcessorRecordContext recordContext;
    private String topic;

    @Before
    public void setUp() {
        final String storeName = "store";
        underlyingStore = new InMemoryKeyValueStore<>(storeName, Serdes.Bytes(), Serdes.ByteArray());
        cacheFlushListener = new CacheFlushListenerStub<>();
        store = new CachingTimestampedKeyValueStore<>(underlyingStore, Serdes.String(), Serdes.String());
        store.setFlushListener(cacheFlushListener, false);
        cache = new ThreadCache(new LogContext("testCache "), maxCacheSizeBytes, new MockStreamsMetrics(new Metrics()));
        final InternalMockProcessorContext context = new InternalMockProcessorContext(null, null, null, null, cache);
        topic = "topic";
        recordContext = new ProcessorRecordContext(-1, 0, 0, "topic", null);
        context.setRecordContext(recordContext);
        store.init(context, null);
    }

    private byte[] bytesValueAndTimestamp(final String value,
                                          final long timestamp) {
        final byte[] rawValue = value.getBytes();
        final ByteBuffer buffer = ByteBuffer.allocate(rawValue.length + 8);
        return buffer.putLong(timestamp).put(rawValue).array();
    }

    private Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() {
        final int added = addItemsToCache();
        // all dirty entries should have been flushed
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, store.approximateNumEntries());
        assertNotNull(underlyingStore.get(Bytes.wrap("0".getBytes())));
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() {
        final int numRecords = addItemsToCache();
        assertEquals(numRecords, cacheFlushListener.forwarded.size());
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        recordContext.setTimestamp(42L);
        store.put(bytesKey("1"), bytesValueAndTimestamp("a", 42L));
        store.flush();
        assertEquals(
            ValueAndTimestamp.make("a", 42L),
            cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() {
        store.setFlushListener(cacheFlushListener, true);
        store.put(bytesKey("1"), bytesValueAndTimestamp("a", 21L));
        store.flush();
        store.put(bytesKey("1"), bytesValueAndTimestamp("b", 42L));
        store.flush();
        assertEquals(
            ValueAndTimestamp.make("b", 42L),
            cacheFlushListener.forwarded.get("1").newValue);
        assertEquals(
            ValueAndTimestamp.make("a", -1L),
            cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldNotForwardOldValuesWhenDisabled() {
        store.put(bytesKey("1"), bytesValueAndTimestamp("a", 21L));
        store.flush();
        store.put(bytesKey("1"), bytesValueAndTimestamp("b", 42L));
        store.flush();
        assertEquals(
            ValueAndTimestamp.make("b", 42L),
            cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldIterateAllStoredItems() {
        final int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> all = store.all();
        final List<Bytes> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldIterateOverRange() {
        final int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> range = store.range(bytesKey(String.valueOf(0)), bytesKey(String.valueOf(items)));
        final List<Bytes> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldDeleteItemsFromCache() {
        store.put(bytesKey("a"), bytesValueAndTimestamp("a", 1L));
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        assertFalse(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.all().hasNext());
    }

    @Test
    public void shouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() {
        store.put(bytesKey("a"), bytesValueAndTimestamp("a", 21L));
        store.flush();
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        assertFalse(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.all().hasNext());
    }

    @Test
    public void shouldClearNamespaceCacheOnClose() {
        store.put(bytesKey("a"), bytesValueAndTimestamp("a", 42L));
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
        store.put(bytesKey("a"), bytesValueAndTimestamp("a", 21L));
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
        store.putAll(Collections.singletonList(KeyValue.pair(bytesKey("a"), bytesValueAndTimestamp("a", 1L))));
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDoPutIfAbsentClosedCachingStore() {
        store.close();
        store.putIfAbsent(bytesKey("b"), bytesValueAndTimestamp("c", 21L));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutWithNullKey() {
        store.put(null, bytesValueAndTimestamp("c", 21L));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey() {
        store.putIfAbsent(null, bytesValueAndTimestamp("c", 21L));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutAllWithNullKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(null, bytesValueAndTimestamp("a", 21L)));
        try {
            store.putAll(entries);
            fail("Should have thrown NullPointerException while putAll null key");
        } catch (final NullPointerException expected) {
        }
    }

    @Test
    public void shouldPutIfAbsent() {
        store.putIfAbsent(bytesKey("b"), bytesValueAndTimestamp("2", 21L));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValueAndTimestamp("2", 21L)));

        store.putIfAbsent(bytesKey("b"), bytesValueAndTimestamp("3", 42L));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValueAndTimestamp("2", 21L)));
    }

    @Test
    public void shouldPutAll() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(bytesKey("a"), bytesValueAndTimestamp("1", 1L)));
        entries.add(new KeyValue<>(bytesKey("b"), bytesValueAndTimestamp("2", 2L)));
        store.putAll(entries);
        assertThat(store.get(bytesKey("a")), equalTo(bytesValueAndTimestamp("1", 1L)));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValueAndTimestamp("2", 2L)));
    }

    @Test
    public void shouldReturnUnderlying() {
        assertEquals(underlyingStore, store.wrapped());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowIfTryingToDeleteFromClosedCachingStore() {
        store.close();
        store.delete(bytesKey("key"));
    }

    private int addItemsToCache() {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            final String kv = String.valueOf(i++);
            store.put(bytesKey(kv), bytesValueAndTimestamp(kv, i));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic);
        }
        return i;
    }

    public static class CacheFlushListenerStub<K, V> implements CacheFlushListener<K, V> {
        final Map<K, Change<ValueAndTimestamp<V>>> forwarded = new HashMap<>();

        @Override
        public void apply(final K key,
                          final V newValue,
                          final V oldValue,
                          final long timestamp) {
            forwarded.put(key, new Change<>(
                ValueAndTimestamp.make(newValue, timestamp),
                ValueAndTimestamp.make(oldValue, -1L)));
        }
    }
}