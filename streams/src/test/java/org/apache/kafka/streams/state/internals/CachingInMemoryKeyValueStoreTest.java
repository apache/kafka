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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CachingInMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private final static String TOPIC = "topic";
    private static final String CACHE_NAMESPACE = "0_0-store-name";
    private final int maxCacheSizeBytes = 150;
    private InternalMockProcessorContext context;
    private CachingKeyValueStore store;
    private KeyValueStore<Bytes, byte[]> underlyingStore;
    private ThreadCache cache;
    private CacheFlushListenerStub<String, String> cacheFlushListener;

    @Before
    public void setUp() {
        final String storeName = "store";
        underlyingStore = new InMemoryKeyValueStore(storeName);
        cacheFlushListener = new CacheFlushListenerStub<>(new StringDeserializer(), new StringDeserializer());
        store = new CachingKeyValueStore(underlyingStore);
        store.setFlushListener(cacheFlushListener, false);
        cache = new ThreadCache(new LogContext("testCache "), maxCacheSizeBytes, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext(null, null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, null));
        store.init((StateStoreContext) context, null);
    }

    @After
    public void after() {
        super.after();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("cache-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde())
                .withCachingEnabled();

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final KeyValueStore<Bytes, byte[]> inner = EasyMock.mock(InMemoryKeyValueStore.class);
        final CachingKeyValueStore outer = new CachingKeyValueStore(inner);
        EasyMock.expect(inner.name()).andStubReturn("store");
        inner.init((ProcessorContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        outer.init((ProcessorContext) context, outer);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        final KeyValueStore<Bytes, byte[]> inner = EasyMock.mock(InMemoryKeyValueStore.class);
        final CachingKeyValueStore outer = new CachingKeyValueStore(inner);
        EasyMock.expect(inner.name()).andStubReturn("store");
        inner.init((StateStoreContext) context, outer);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        outer.init((StateStoreContext) context, outer);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldSetFlushListener() {
        assertTrue(store.setFlushListener(null, true));
        assertTrue(store.setFlushListener(null, false));
    }

    @Test
    public void shouldAvoidFlushingDeletionsWithoutDirtyKeys() {
        final int added = addItemsToCache();
        // all dirty entries should have been flushed
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, cacheFlushListener.forwarded.size());

        store.put(bytesKey("key"), bytesValue("value"));
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, cacheFlushListener.forwarded.size());

        store.put(bytesKey("key"), null);
        store.flush();
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, cacheFlushListener.forwarded.size());
    }

    @Test
    public void shouldCloseWrappedStoreAndCacheAfterErrorDuringCacheFlush() {
        setUpCloseTests();
        EasyMock.reset(cache);
        cache.flush(CACHE_NAMESPACE);
        EasyMock.expectLastCall().andThrow(new RuntimeException("Simulating an error on flush"));
        EasyMock.replay(cache);
        EasyMock.reset(underlyingStore);
        underlyingStore.close();
        EasyMock.replay(underlyingStore);

        assertThrows(RuntimeException.class, store::close);
        EasyMock.verify(cache, underlyingStore);
    }

    @Test
    public void shouldCloseWrappedStoreAfterErrorDuringCacheClose() {
        setUpCloseTests();
        EasyMock.reset(cache);
        cache.flush(CACHE_NAMESPACE);
        cache.close(CACHE_NAMESPACE);
        EasyMock.expectLastCall().andThrow(new RuntimeException("Simulating an error on close"));
        EasyMock.replay(cache);
        EasyMock.reset(underlyingStore);
        underlyingStore.close();
        EasyMock.replay(underlyingStore);

        assertThrows(RuntimeException.class, store::close);
        EasyMock.verify(cache, underlyingStore);
    }

    @Test
    public void shouldCloseCacheAfterErrorDuringStateStoreClose() {
        setUpCloseTests();
        EasyMock.reset(cache);
        cache.flush(CACHE_NAMESPACE);
        cache.close(CACHE_NAMESPACE);
        EasyMock.replay(cache);
        EasyMock.reset(underlyingStore);
        underlyingStore.close();
        EasyMock.expectLastCall().andThrow(new RuntimeException("Simulating an error on close"));
        EasyMock.replay(underlyingStore);

        assertThrows(RuntimeException.class, store::close);
        EasyMock.verify(cache, underlyingStore);
    }

    private void setUpCloseTests() {
        underlyingStore = EasyMock.createNiceMock(KeyValueStore.class);
        EasyMock.expect(underlyingStore.name()).andStubReturn("store-name");
        EasyMock.expect(underlyingStore.isOpen()).andStubReturn(true);
        EasyMock.replay(underlyingStore);
        store = new CachingKeyValueStore(underlyingStore);
        cache = EasyMock.niceMock(ThreadCache.class);
        context = new InternalMockProcessorContext(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, null));
        store.init((StateStoreContext) context, store);
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
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), bytesValue("c"));
        store.flush();
        assertEquals("c", cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("a", cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("c", cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.clear();
        store.put(bytesKey("1"), bytesValue("a"));
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.clear();
    }

    @Test
    public void shouldNotForwardOldValuesWhenDisabled() {
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), bytesValue("b"));
        store.flush();
        assertEquals("b", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.clear();
        store.put(bytesKey("1"), bytesValue("a"));
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.clear();
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
        assertEquals(Arrays.asList(
            Bytes.wrap("0".getBytes()),
            Bytes.wrap("1".getBytes()),
            Bytes.wrap("2".getBytes())
        ), results);
    }

    @Test
    public void shouldReverseIterateAllStoredItems() {
        final int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> all = store.reverseAll();
        final List<Bytes> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        assertEquals(items, results.size());
        assertEquals(Arrays.asList(
            Bytes.wrap("2".getBytes()),
            Bytes.wrap("1".getBytes()),
            Bytes.wrap("0".getBytes())
        ), results);
    }

    @Test
    public void shouldIterateOverRange() {
        final int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> range =
            store.range(bytesKey(String.valueOf(0)), bytesKey(String.valueOf(items)));
        final List<Bytes> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        assertEquals(items, results.size());
        assertEquals(Arrays.asList(
            Bytes.wrap("0".getBytes()),
            Bytes.wrap("1".getBytes()),
            Bytes.wrap("2".getBytes())
        ), results);
    }

    @Test
    public void shouldReverseIterateOverRange() {
        final int items = addItemsToCache();
        final KeyValueIterator<Bytes, byte[]> range =
            store.reverseRange(bytesKey(String.valueOf(0)), bytesKey(String.valueOf(items)));
        final List<Bytes> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        assertEquals(items, results.size());
        assertEquals(Arrays.asList(
            Bytes.wrap("2".getBytes()),
            Bytes.wrap("1".getBytes()),
            Bytes.wrap("0".getBytes())
        ), results);
    }

    @Test
    public void shouldGetRecordsWithPrefixKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(bytesKey("p11"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("k1"), bytesValue("1")));
        entries.add(new KeyValue<>(bytesKey("k2"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("p2"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("p1"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("p0"), bytesValue("2")));

        store.putAll(entries);

        final KeyValueIterator<Bytes, byte[]> keysWithPrefix = store.prefixScan("p1", new StringSerializer());
        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        int numberOfKeysReturned = 0;

        while (keysWithPrefix.hasNext()) {
            final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
            keys.add(next.key.toString());
            values.add(new String(next.value));
            numberOfKeysReturned++;
        }
        assertThat(numberOfKeysReturned, is(2));
        assertThat(keys, is(Arrays.asList("p1", "p11")));
        assertThat(values, is(Arrays.asList("2", "2")));
    }

    @Test
    public void shouldGetRecordsWithPrefixKeyExcludingNextLargestKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(bytesKey("abcd"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("abcdd"), bytesValue("1")));
        entries.add(new KeyValue<>(bytesKey("abce"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("abc"), bytesValue("2")));

        store.putAll(entries);

        final KeyValueIterator<Bytes, byte[]> keysWithPrefix = store.prefixScan("abcd", new StringSerializer());
        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        int numberOfKeysReturned = 0;

        while (keysWithPrefix.hasNext()) {
            final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
            keys.add(next.key.toString());
            values.add(new String(next.value));
            numberOfKeysReturned++;
        }
        assertThat(numberOfKeysReturned, is(2));
        assertThat(keys, is(Arrays.asList("abcd", "abcdd")));
        assertThat(values, is(Arrays.asList("2", "1")));
    }

    @Test
    public void shouldDeleteItemsFromCache() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        assertFalse(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.reverseRange(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.all().hasNext());
        assertFalse(store.reverseAll().hasNext());
    }

    @Test
    public void shouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.flush();
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        assertFalse(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.reverseRange(bytesKey("a"), bytesKey("b")).hasNext());
        assertFalse(store.all().hasNext());
        assertFalse(store.reverseAll().hasNext());
    }

    @Test
    public void shouldClearNamespaceCacheOnClose() {
        store.put(bytesKey("a"), bytesValue("a"));
        assertEquals(1, cache.size());
        store.close();
        assertEquals(0, cache.size());
    }

    @Test
    public void shouldThrowIfTryingToGetFromClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.get(bytesKey("a"));
        });
    }

    @Test
    public void shouldThrowIfTryingToWriteToClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.put(bytesKey("a"), bytesValue("a"));
        });
    }

    @Test
    public void shouldThrowIfTryingToDoRangeQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.range(bytesKey("a"), bytesKey("b"));
        });
    }

    @Test
    public void shouldThrowIfTryingToDoReverseRangeQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.reverseRange(bytesKey("a"), bytesKey("b"));
        });
    }

    @Test
    public void shouldThrowIfTryingToDoAllQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.all();
        });
    }

    @Test
    public void shouldThrowIfTryingToDoReverseAllQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.reverseAll();
        });
    }

    @Test
    public void shouldThrowIfTryingToDoGetApproxSizeOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.close();
            store.approximateNumEntries();
        });
    }

    @Test
    public void shouldThrowIfTryingToDoPutAllClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.putAll(Collections.singletonList(KeyValue.pair(bytesKey("a"), bytesValue("a"))));
        });
    }

    @Test
    public void shouldThrowIfTryingToDoPutIfAbsentClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.putIfAbsent(bytesKey("b"), bytesValue("c"));
        });
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutWithNullKey() {
        assertThrows(NullPointerException.class, () -> store.put(null, bytesValue("c")));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey() {
        assertThrows(NullPointerException.class, () -> store.putIfAbsent(null, bytesValue("c")));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutAllWithNullKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(null, bytesValue("a")));
        assertThrows(NullPointerException.class, () -> store.putAll(entries));
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
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(bytesKey("a"), bytesValue("1")));
        entries.add(new KeyValue<>(bytesKey("b"), bytesValue("2")));
        store.putAll(entries);
        assertThat(store.get(bytesKey("a")), equalTo(bytesValue("1")));
        assertThat(store.get(bytesKey("b")), equalTo(bytesValue("2")));
    }

    @Test
    public void shouldReturnUnderlying() {
        assertEquals(underlyingStore, store.wrapped());
    }

    @Test
    public void shouldThrowIfTryingToDeleteFromClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.delete(bytesKey("key"));
        });
    }

    private int addItemsToCache() {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            final String kv = String.valueOf(i++);
            store.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), TOPIC);
        }
        return i;
    }

}
