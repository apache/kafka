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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class MergedSortedCacheKeyValueStoreIteratorTest {

    private final String namespace = "one";
    private final StateSerdes<byte[], byte[]> serdes =  new StateSerdes<>(namespace, Serdes.ByteArray(), Serdes.ByteArray());
    private KeyValueStore<Bytes, byte[]> store;
    private ThreadCache cache;

    @Before
    public void setUp() throws Exception {
        store = new InMemoryKeyValueStore<>(namespace);
        cache = new ThreadCache(10000L);
    }

    @Test
    public void shouldIterateOverRange() throws Exception {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (int i = 0; i < bytes.length; i += 2) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, bytes[i + 1], new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[]{2});
        final Bytes to = Bytes.wrap(new byte[]{9});
        final PeekingKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(store.range(from, to));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from.get(), to.get());

        final MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> iterator = new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, serdes);
        byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (iterator.hasNext()) {
            final byte[] value = iterator.next().value;
            values[index++] = value;
            assertArrayEquals(bytes[bytesIndex++], value);
        }
    }


    @Test
    public void shouldSkipLargerDeletedCacheValue() throws Exception {
        final byte[][] bytes = {{0}, {1}};
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        cache.put(namespace, bytes[1], new LRUCacheEntry(null));
        final MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> iterator = createIterator();
        assertArrayEquals(bytes[0], iterator.next().key);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldSkipSmallerDeletedCachedValue() throws Exception {
        final byte[][] bytes = {{0}, {1}};
        cache.put(namespace, bytes[0], new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[1]), bytes[1]);
        final MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> iterator = createIterator();
        assertArrayEquals(bytes[1], iterator.next().key);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldIgnoreIfDeletedInCacheButExistsInStore() throws Exception {
        final byte[][] bytes = {{0}};
        cache.put(namespace, bytes[0], new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        final MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> iterator = createIterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotHaveNextIfAllCachedItemsDeleted() throws Exception {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (int i = 0; i < bytes.length; i++) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, bytes[i], new LRUCacheEntry(null));
        }
        assertFalse(createIterator().hasNext());
    }

    @Test
    public void shouldNotHaveNextIfOnlyCacheItemsAndAllDeleted() throws Exception {
        final byte[][] bytes = {{0}, {1}, {2}};
        for (int i = 0; i < bytes.length; i++) {
            cache.put(namespace, bytes[i], new LRUCacheEntry(null));
        }
        assertFalse(createIterator().hasNext());
    }

    @Test
    public void shouldSkipAllDeletedFromCache() throws Exception {
        final byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (int i = 0; i < bytes.length; i++) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, bytes[i], new LRUCacheEntry(bytes[i]));
        }
        cache.put(namespace, bytes[1], new LRUCacheEntry(null));
        cache.put(namespace, bytes[2], new LRUCacheEntry(null));
        cache.put(namespace, bytes[3], new LRUCacheEntry(null));
        cache.put(namespace, bytes[8], new LRUCacheEntry(null));
        cache.put(namespace, bytes[11], new LRUCacheEntry(null));

        final MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> iterator = createIterator();
        assertArrayEquals(bytes[0], iterator.next().key);
        assertArrayEquals(bytes[4], iterator.next().key);
        assertArrayEquals(bytes[5], iterator.next().key);
        assertArrayEquals(bytes[6], iterator.next().key);
        assertArrayEquals(bytes[7], iterator.next().key);
        assertArrayEquals(bytes[9], iterator.next().key);
        assertArrayEquals(bytes[10], iterator.next().key);
        assertFalse(iterator.hasNext());

    }

    private MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> createIterator() {
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(namespace);
        final PeekingKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(store.all());
        return new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, serdes);
    }


}