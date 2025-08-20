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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


public class MergedSortedCacheKeyValueBytesStoreIteratorTest {

    private final String namespace = "0.0-one";
    private KeyValueStore<Bytes, byte[]> store;
    private ThreadCache cache;

    @BeforeEach
    public void setUp() {
        store = new InMemoryKeyValueStore(namespace);
        cache = new ThreadCache(new LogContext("testCache "), 10000L, new MockStreamsMetrics(new Metrics()));
    }
    @Test
    public void shouldIterateOverRange() {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (int i = 0; i < bytes.length; i += 2) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[] {2});
        final Bytes to = Bytes.wrap(new byte[] {9});
        final KeyValueIterator<Bytes, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>("store", store.range(from, to));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from, to);

        final MergedSortedCacheKeyValueBytesStoreIterator iterator =
            new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
        final byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (iterator.hasNext()) {
            final byte[] value = iterator.next().value;
            values[index++] = value;
            assertArrayEquals(bytes[bytesIndex++], value);
        }
        iterator.close();
    }


    @Test
    public void shouldReverseIterateOverRange() {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (int i = 0; i < bytes.length; i += 2) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[] {2});
        final Bytes to = Bytes.wrap(new byte[] {9});
        final KeyValueIterator<Bytes, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>("store", store.reverseRange(from, to));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.reverseRange(namespace, from, to);

        final MergedSortedCacheKeyValueBytesStoreIterator iterator =
            new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, false);
        final byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 9;
        while (iterator.hasNext()) {
            final byte[] value = iterator.next().value;
            values[index++] = value;
            assertArrayEquals(bytes[bytesIndex--], value);
        }
        iterator.close();
    }

    @Test
    public void shouldSkipLargerDeletedCacheValue() {
        final byte[][] bytes = {{0}, {1}};
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        cache.put(namespace, Bytes.wrap(bytes[1]), new LRUCacheEntry(null));
        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertArrayEquals(bytes[0], iterator.next().key.get());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldSkipSmallerDeletedCachedValue() {
        final byte[][] bytes = {{0}, {1}};
        cache.put(namespace, Bytes.wrap(bytes[0]), new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[1]), bytes[1]);
        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertArrayEquals(bytes[1], iterator.next().key.get());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldIgnoreIfDeletedInCacheButExistsInStore() {
        final byte[][] bytes = {{0}};
        cache.put(namespace, Bytes.wrap(bytes[0]), new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldNotHaveNextIfAllCachedItemsDeleted() {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (final byte[] aByte : bytes) {
            final Bytes aBytes = Bytes.wrap(aByte);
            store.put(aBytes, aByte);
            cache.put(namespace, aBytes, new LRUCacheEntry(null));
        }
        assertFalse(createIterator().hasNext());
    }

    @Test
    public void shouldNotHaveNextIfOnlyCacheItemsAndAllDeleted() {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (final byte[] aByte : bytes) {
            cache.put(namespace, Bytes.wrap(aByte), new LRUCacheEntry(null));
        }
        assertFalse(createIterator().hasNext());
    }

    @Test
    public void shouldIterateCacheOnly() {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (final byte[] aByte : bytes) {
            cache.put(namespace, Bytes.wrap(aByte), new LRUCacheEntry(aByte));
        }

        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertArrayEquals(bytes[0], iterator.next().key.get());
            assertArrayEquals(bytes[1], iterator.next().key.get());
            assertArrayEquals(bytes[2], iterator.next().key.get());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldIterateStoreOnly() {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (final byte[] aByte : bytes) {
            store.put(Bytes.wrap(aByte), aByte);
        }

        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertArrayEquals(bytes[0], iterator.next().key.get());
            assertArrayEquals(bytes[1], iterator.next().key.get());
            assertArrayEquals(bytes[2], iterator.next().key.get());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldSkipAllDeletedFromCache() {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (final byte[] aByte : bytes) {
            final Bytes aBytes = Bytes.wrap(aByte);
            store.put(aBytes, aByte);
        }

        cache.put(namespace, Bytes.wrap(new byte[]{-1}), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[1]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[2]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[3]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[8]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[11]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(new byte[]{14}), new LRUCacheEntry(null));

        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertArrayEquals(bytes[0], iterator.next().key.get());
            assertArrayEquals(bytes[4], iterator.next().key.get());
            assertArrayEquals(bytes[5], iterator.next().key.get());
            assertArrayEquals(bytes[6], iterator.next().key.get());
            assertArrayEquals(bytes[7], iterator.next().key.get());
            assertArrayEquals(bytes[9], iterator.next().key.get());
            assertArrayEquals(bytes[10], iterator.next().key.get());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldNotHaveNextIfBothIteratorsInitializedEmpty() {
        try (final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator()) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldPeekNextKey() {
        final KeyValueStore<Bytes, byte[]> kv = new InMemoryKeyValueStore("one");
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000000L, new MockStreamsMetrics(new Metrics()));
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        for (int i = 0; i < bytes.length - 1; i += 2) {
            kv.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[] {2});
        final Bytes to = Bytes.wrap(new byte[] {9});
        final KeyValueIterator<Bytes, byte[]> storeIterator = kv.range(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from, to);

        final MergedSortedCacheKeyValueBytesStoreIterator iterator =
            new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
        final byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (iterator.hasNext()) {
            final byte[] keys = iterator.peekNextKey().get();
            values[index++] = keys;
            assertArrayEquals(bytes[bytesIndex++], keys);
            iterator.next();
        }
        iterator.close();
    }

    @Test
    public void shouldPeekNextKeyReverse() {
        final KeyValueStore<Bytes, byte[]> kv = new InMemoryKeyValueStore("one");
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000000L, new MockStreamsMetrics(new Metrics()));
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        for (int i = 0; i < bytes.length - 1; i += 2) {
            kv.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[] {2});
        final Bytes to = Bytes.wrap(new byte[] {9});
        final KeyValueIterator<Bytes, byte[]> storeIterator = kv.reverseRange(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.reverseRange(namespace, from, to);

        final MergedSortedCacheKeyValueBytesStoreIterator iterator =
            new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, false);
        final byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 9;
        while (iterator.hasNext()) {
            final byte[] keys = iterator.peekNextKey().get();
            values[index++] = keys;
            assertArrayEquals(bytes[bytesIndex--], keys);
            iterator.next();
        }
        iterator.close();
    }

    private MergedSortedCacheKeyValueBytesStoreIterator createIterator() {
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(namespace);
        final KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.all());
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
    }
}