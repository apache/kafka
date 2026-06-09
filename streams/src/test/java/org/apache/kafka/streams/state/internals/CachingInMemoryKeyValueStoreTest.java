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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.ThreadCacheTest.memoryCacheEntrySize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class CachingInMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private static final String TOPIC = "topic";
    private static final String CACHE_NAMESPACE = "0_0-store-name";
    private final int maxCacheSizeBytes = 150;
    private InternalMockProcessorContext<?, ?> context;
    private CachingKeyValueStore store;
    private KeyValueStore<Bytes, byte[]> underlyingStore;
    private ThreadCache cache;
    private CacheFlushListenerStub<String, String> cacheFlushListener;

    @BeforeEach
    public void setUp() {
        final String storeName = "store";
        underlyingStore = new InMemoryKeyValueStore(storeName);
        cacheFlushListener = new CacheFlushListenerStub<>(new StringDeserializer(), new StringDeserializer());
        store = new CachingKeyValueStore(underlyingStore, CachingKeyValueStore.CacheType.KEY_VALUE_STORE);
        store.setFlushListener(cacheFlushListener, false);
        cache = new ThreadCache(new LogContext("testCache "), maxCacheSizeBytes, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext<>(null, null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, new RecordHeaders()));
        store.init(context, null);
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

    @Test
    public void shouldDelegateInit() {
        final KeyValueStore<Bytes, byte[]> inner = mock(InMemoryKeyValueStore.class);
        final CachingKeyValueStore outer = new CachingKeyValueStore(inner, CachingKeyValueStore.CacheType.KEY_VALUE_STORE);
        when(inner.name()).thenReturn("store");
        outer.init(context, outer);
        verify(inner).init(context, outer);
    }

    @Test
    public void shouldSetFlushListener() {
        assertTrue(store.setFlushListener(null, true));
        assertTrue(store.setFlushListener(null, false));
    }

    @Test
    public void shouldAvoidFlushingDeletionsWithoutDirtyKeys() {
        final int added = addItemsToCache();
        // all dirty entries should have been committed
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, cacheFlushListener.forwarded.size());

        store.put(bytesKey("key"), bytesValue("value"));
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, cacheFlushListener.forwarded.size());

        store.put(bytesKey("key"), null);
        store.commit(Map.of());
        assertEquals(added, underlyingStore.approximateNumEntries());
        assertEquals(added, cacheFlushListener.forwarded.size());
    }

    @Test
    public void shouldCloseWrappedStoreAndCacheAfterErrorDuringCacheFlush() {
        setUpCloseTests();
        final InOrder inOrder = inOrder(cache, underlyingStore);
        doThrow(new RuntimeException("Simulating an error on flush")).when(cache).flush(CACHE_NAMESPACE);

        assertThrows(RuntimeException.class, store::close);

        inOrder.verify(cache).close(CACHE_NAMESPACE);
        inOrder.verify(underlyingStore).close();
    }

    @Test
    public void shouldCloseWrappedStoreAfterErrorDuringCacheClose() {
        setUpCloseTests();
        final InOrder inOrder = inOrder(cache, underlyingStore);
        doThrow(new RuntimeException("Simulating an error on close")).when(cache).close(CACHE_NAMESPACE);

        assertThrows(RuntimeException.class, store::close);

        inOrder.verify(cache).flush(CACHE_NAMESPACE);
        inOrder.verify(underlyingStore).close();
    }

    @Test
    public void shouldCloseCacheAfterErrorDuringStateStoreClose() {
        setUpCloseTests();
        final InOrder inOrder = inOrder(cache);
        doThrow(new RuntimeException("Simulating an error on close")).when(underlyingStore).close();

        assertThrows(RuntimeException.class, store::close);

        inOrder.verify(cache).flush(CACHE_NAMESPACE);
        inOrder.verify(cache).close(CACHE_NAMESPACE);
    }

    @SuppressWarnings("unchecked")
    private void setUpCloseTests() {
        underlyingStore = mock(KeyValueStore.class);
        when(underlyingStore.name()).thenReturn("store-name");
        store = new CachingKeyValueStore(underlyingStore, CachingKeyValueStore.CacheType.TIMESTAMPED_KEY_VALUE_STORE);
        cache = mock(ThreadCache.class);
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, new RecordHeaders()));
        store.init(context, store);
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

    @Test
    public void shouldMatchPositionAfterPutWithFlushListener() {
        store.setFlushListener(record -> { }, false);
        shouldMatchPositionAfterPut();
    }

    @Test
    public void shouldMatchPositionAfterPutWithoutFlushListener() {
        store.setFlushListener(null, false);
        shouldMatchPositionAfterPut();
    }

    private void shouldMatchPositionAfterPut() {
        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        store.put(bytesKey("key1"), bytesValue("value1"));
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        store.put(bytesKey("key2"), bytesValue("value2"));

        // Position should correspond to the last record's context, not the current context.
        context.setRecordContext(
            new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders())
        );

        assertEquals(
            Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 2L))))),
            store.getPosition()
        );
        assertEquals(Position.emptyPosition(), underlyingStore.getPosition());

        store.commit(Map.of());

        assertEquals(
            Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 2L))))),
            store.getPosition()
        );
        assertEquals(
            Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 2L))))),
            underlyingStore.getPosition()
        );
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
        store.commit(Map.of());
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() {
        store.setFlushListener(cacheFlushListener, true);
        store.put(bytesKey("1"), bytesValue("a"));
        store.commit(Map.of());
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), bytesValue("c"));
        store.commit(Map.of());
        assertEquals("c", cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("a", cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), null);
        store.commit(Map.of());
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("c", cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.clear();
        store.put(bytesKey("1"), bytesValue("a"));
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), null);
        store.commit(Map.of());
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.clear();
    }

    @Test
    public void shouldNotForwardOldValuesWhenDisabled() {
        store.put(bytesKey("1"), bytesValue("a"));
        store.commit(Map.of());
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), bytesValue("b"));
        store.commit(Map.of());
        assertEquals("b", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), null);
        store.commit(Map.of());
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.clear();
        store.put(bytesKey("1"), bytesValue("a"));
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), null);
        store.commit(Map.of());
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.clear();
    }

    @Test
    public void shouldIterateAllStoredItems() {
        final int items = addItemsToCache();
        final List<Bytes> results = new ArrayList<>();

        try (final KeyValueIterator<Bytes, byte[]> all = store.all()) {
            while (all.hasNext()) {
                results.add(all.next().key);
            }
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
        final List<Bytes> results = new ArrayList<>();

        try (final KeyValueIterator<Bytes, byte[]> all = store.reverseAll()) {
            while (all.hasNext()) {
                results.add(all.next().key);
            }
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
        final List<Bytes> results = new ArrayList<>();

        try (final KeyValueIterator<Bytes, byte[]> range =
                 store.range(bytesKey(String.valueOf(0)), bytesKey(String.valueOf(items)))) {
            while (range.hasNext()) {
                results.add(range.next().key);
            }
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
        final List<Bytes> results = new ArrayList<>();

        try (final KeyValueIterator<Bytes, byte[]> range =
                 store.reverseRange(bytesKey(String.valueOf(0)), bytesKey(String.valueOf(items)))) {
            while (range.hasNext()) {
                results.add(range.next().key);
            }
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

        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = store.prefixScan("p1", new StringSerializer())) {
            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                keys.add(next.key.toString());
                values.add(new String(next.value));
                numberOfKeysReturned++;
            }
        }

        assertEquals(2, numberOfKeysReturned);
        assertEquals(Arrays.asList("p1", "p11"), keys);
        assertEquals(Arrays.asList("2", "2"), values);

    }

    @Test
    public void shouldGetRecordsWithPrefixKeyExcludingNextLargestKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(bytesKey("abcd"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("abcdd"), bytesValue("1")));
        entries.add(new KeyValue<>(bytesKey("abce"), bytesValue("2")));
        entries.add(new KeyValue<>(bytesKey("abc"), bytesValue("2")));

        store.putAll(entries);

        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = store.prefixScan("abcd", new StringSerializer())) {
            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                keys.add(next.key.toString());
                values.add(new String(next.value));
                numberOfKeysReturned++;
            }
        }

        assertEquals(2, numberOfKeysReturned);
        assertEquals(Arrays.asList("abcd", "abcdd"), keys);
        assertEquals(Arrays.asList("2", "1"), values);
    }

    @Test
    public void shouldDeleteItemsFromCache() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.range(bytesKey("a"), bytesKey("b"))) {
            assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.reverseRange(bytesKey("a"), bytesKey("b"))) {
            assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.all()) {
            assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.reverseAll()) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.commit(Map.of());
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.range(bytesKey("a"), bytesKey("b"))) {
            assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.reverseRange(bytesKey("a"), bytesKey("b"))) {
            assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.all()) {
            assertFalse(iterator.hasNext());
        }
        try (final KeyValueIterator<Bytes, byte[]> iterator = store.reverseAll()) {
            assertFalse(iterator.hasNext());
        }
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

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowIfTryingToDoRangeQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.range(bytesKey("a"), bytesKey("b"));
        });
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowIfTryingToDoReverseRangeQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.reverseRange(bytesKey("a"), bytesKey("b"));
        });
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowIfTryingToDoAllQueryOnClosedCachingStore() {
        assertThrows(InvalidStateStoreException.class, () -> {
            store.close();
            store.all();
        });
    }

    @SuppressWarnings("resource")
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

    // readOnly(IsolationLevel) tests

    @Test
    public void shouldReadCommittedBypassCacheForGet() {
        // cache-only entry should be invisible under READ_COMMITTED
        store.put(bytesKey("cache-only"), bytesValue("v"));
        assertEquals(0, underlyingStore.approximateNumEntries());
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_COMMITTED);
        assertNull(view.get(bytesKey("cache-only")));

        // after flush the entry is in the underlying store and visible
        store.commit(Map.of());
        assertNotNull(view.get(bytesKey("cache-only")));
    }

    @Test
    public void shouldReadUncommittedViewGetFromCacheOnly() {
        store.put(bytesKey("c"), bytesValue("cache-val"));
        assertEquals(0, underlyingStore.approximateNumEntries());
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        assertThat(view.get(bytesKey("c")), equalTo(bytesValue("cache-val")));
    }

    @Test
    public void shouldReadUncommittedViewGetFromStoreOnly() {
        underlyingStore.put(bytesKey("s"), bytesValue("store-val"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        assertThat(view.get(bytesKey("s")), equalTo(bytesValue("store-val")));
    }

    @Test
    public void shouldReadUncommittedViewGetCacheShadowsStore() {
        underlyingStore.put(bytesKey("k"), bytesValue("store-val"));
        store.put(bytesKey("k"), bytesValue("cache-val"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        assertThat(view.get(bytesKey("k")), equalTo(bytesValue("cache-val")));
    }

    @Test
    public void shouldReadUncommittedViewRange() {
        underlyingStore.put(bytesKey("a"), bytesValue("1"));
        store.put(bytesKey("b"), bytesValue("2"));
        underlyingStore.put(bytesKey("c"), bytesValue("3"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        final List<Bytes> keys = new ArrayList<>();
        try (final KeyValueIterator<Bytes, byte[]> it = view.range(bytesKey("a"), bytesKey("c"))) {
            while (it.hasNext()) {
                keys.add(it.next().key);
            }
        }
        assertEquals(Arrays.asList(bytesKey("a"), bytesKey("b"), bytesKey("c")), keys);
    }

    @Test
    public void shouldReadUncommittedViewReverseRange() {
        underlyingStore.put(bytesKey("a"), bytesValue("1"));
        store.put(bytesKey("b"), bytesValue("2"));
        underlyingStore.put(bytesKey("c"), bytesValue("3"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        final List<Bytes> keys = new ArrayList<>();
        try (final KeyValueIterator<Bytes, byte[]> it = view.reverseRange(bytesKey("a"), bytesKey("c"))) {
            while (it.hasNext()) {
                keys.add(it.next().key);
            }
        }
        assertEquals(Arrays.asList(bytesKey("c"), bytesKey("b"), bytesKey("a")), keys);
    }

    @Test
    public void shouldReadUncommittedViewAll() {
        underlyingStore.put(bytesKey("a"), bytesValue("1"));
        store.put(bytesKey("b"), bytesValue("2"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        final List<Bytes> keys = new ArrayList<>();
        try (final KeyValueIterator<Bytes, byte[]> it = view.all()) {
            while (it.hasNext()) {
                keys.add(it.next().key);
            }
        }
        assertEquals(Arrays.asList(bytesKey("a"), bytesKey("b")), keys);
    }

    @Test
    public void shouldReadUncommittedViewReverseAll() {
        underlyingStore.put(bytesKey("a"), bytesValue("1"));
        store.put(bytesKey("b"), bytesValue("2"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        final List<Bytes> keys = new ArrayList<>();
        try (final KeyValueIterator<Bytes, byte[]> it = view.reverseAll()) {
            while (it.hasNext()) {
                keys.add(it.next().key);
            }
        }
        assertEquals(Arrays.asList(bytesKey("b"), bytesKey("a")), keys);
    }

    @Test
    public void shouldReadUncommittedViewPrefixScan() {
        underlyingStore.put(bytesKey("foo1"), bytesValue("1"));
        store.put(bytesKey("foo2"), bytesValue("2"));
        underlyingStore.put(bytesKey("bar1"), bytesValue("3"));
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        final List<Bytes> keys = new ArrayList<>();
        try (final KeyValueIterator<Bytes, byte[]> it = view.prefixScan("foo", new StringSerializer())) {
            while (it.hasNext()) {
                keys.add(it.next().key);
            }
        }
        assertEquals(Arrays.asList(bytesKey("foo1"), bytesKey("foo2")), keys);
    }

    @Test
    public void shouldReadUncommittedViewApproximateNumEntriesDelegatesToUnderlying() {
        underlyingStore.put(bytesKey("a"), bytesValue("1"));
        store.put(bytesKey("b"), bytesValue("2")); // cache only, not in underlying
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        assertEquals(underlyingStore.approximateNumEntries(), view.approximateNumEntries());
    }

    @Test
    public void shouldReturnEmptyAndWarnOnInvertedRangeOnOuter() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CachingKeyValueStore.class);
             final KeyValueIterator<Bytes, byte[]> it = store.range(bytesKey("z"), bytesKey("a"))) {
            assertFalse(it.hasNext());
            assertThat(appender.getMessages(), hasItem(
                "Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers"));
        }
    }

    @Test
    public void shouldReturnEmptyAndWarnOnInvertedRangeViaView() {
        final ReadOnlyKeyValueStore<Bytes, byte[]> view = store.readOnly(IsolationLevel.READ_UNCOMMITTED);
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CachingKeyValueStore.class);
             final KeyValueIterator<Bytes, byte[]> it = view.range(bytesKey("z"), bytesKey("a"))) {
            assertFalse(it.hasNext());
            assertThat(appender.getMessages(), hasItem(
                "Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers"));
        }
    }

    private int addItemsToCache() {
        long cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            final String kv = String.valueOf(i++);
            store.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), TOPIC);
        }
        return i;
    }

}
