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
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class MergedSortedCacheKeyValueBytesStoreIteratorTest {

    private final String namespace = "0.0-one";
    private KeyValueStore<Bytes, byte[]> store;
    private ThreadCache cache;

    @Before
    public void setUp() {
        store = new InMemoryKeyValueStore<>(namespace, Serdes.Bytes(), Serdes.ByteArray());
        cache = new ThreadCache(new LogContext("testCache "), 10000L, new MockStreamsMetrics(new Metrics()));
        cache.createCache(namespace);
    }

    @Test
    public void shouldIterateOverRange() {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (int i = 0; i < bytes.length; i += 2) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[]{2});
        final Bytes to = Bytes.wrap(new byte[]{9});
        final KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.range(from, to));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from, to);

        final MergedSortedCacheKeyValueBytesStoreIterator iterator = new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
        byte[][] values = new byte[8][];
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
    public void shouldSkipLargerDeletedCacheValue() {
        final byte[][] bytes = {{0}, {1}};
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        cache.put(namespace, Bytes.wrap(bytes[1]), new LRUCacheEntry(null));
        final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertArrayEquals(bytes[0], iterator.next().key.get());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldSkipSmallerDeletedCachedValue() {
        final byte[][] bytes = {{0}, {1}};
        cache.put(namespace, Bytes.wrap(bytes[0]), new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[1]), bytes[1]);
        final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertArrayEquals(bytes[1], iterator.next().key.get());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldIgnoreIfDeletedInCacheButExistsInStore() {
        final byte[][] bytes = {{0}};
        cache.put(namespace, Bytes.wrap(bytes[0]), new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotHaveNextIfAllCachedItemsDeleted() {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (byte[] aByte : bytes) {
            Bytes aBytes = Bytes.wrap(aByte);
            store.put(aBytes, aByte);
            cache.put(namespace, aBytes, new LRUCacheEntry(null));
        }
        assertFalse(createIterator().hasNext());
    }

    @Test
    public void shouldNotHaveNextIfOnlyCacheItemsAndAllDeleted() {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (byte[] aByte : bytes) {
            cache.put(namespace, Bytes.wrap(aByte), new LRUCacheEntry(null));
        }
        assertFalse(createIterator().hasNext());
    }

    @Test
    public void shouldSkipAllDeletedFromCache() {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (byte[] aByte : bytes) {
            Bytes aBytes = Bytes.wrap(aByte);
            store.put(aBytes, aByte);
            cache.put(namespace, aBytes, new LRUCacheEntry(aByte));
        }
        cache.put(namespace, Bytes.wrap(bytes[1]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[2]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[3]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[8]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[11]), new LRUCacheEntry(null));

        final MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertArrayEquals(bytes[0], iterator.next().key.get());
        assertArrayEquals(bytes[4], iterator.next().key.get());
        assertArrayEquals(bytes[5], iterator.next().key.get());
        assertArrayEquals(bytes[6], iterator.next().key.get());
        assertArrayEquals(bytes[7], iterator.next().key.get());
        assertArrayEquals(bytes[9], iterator.next().key.get());
        assertArrayEquals(bytes[10], iterator.next().key.get());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void shouldPeekNextKey() {
        final KeyValueStore<Bytes, byte[]> kv = new InMemoryKeyValueStore<>("one", Serdes.Bytes(), Serdes.ByteArray());
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000000L, new MockStreamsMetrics(new Metrics()));
        cache.createCache(namespace);
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        for (int i = 0; i < bytes.length - 1; i += 2) {
            kv.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[]{2});
        final Bytes to = Bytes.wrap(new byte[]{9});
        final KeyValueIterator<Bytes, byte[]> storeIterator = kv.range(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from, to);

        final MergedSortedCacheKeyValueBytesStoreIterator iterator =
                new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator,
                                                                storeIterator
                );
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

    private MergedSortedCacheKeyValueBytesStoreIterator createIterator() {
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(namespace);
        final KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.all());
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
    }
}