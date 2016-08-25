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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <V>
 */
class MergedSortedCacheKeyValueStoreIterator<K, V> implements KeyValueIterator<K, V> {
    private final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator cacheIter;
    private final PeekingKeyValueIterator<Bytes, byte[]> storeIterator;
    private final KeyValueStore<Bytes, byte[]> store;
    private final StateSerdes<K, V> serdes;
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

    public MergedSortedCacheKeyValueStoreIterator(final KeyValueStore<Bytes, byte[]> store,
                                                  final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator cacheIter,
                                                  final PeekingKeyValueIterator<Bytes, byte[]> storeIterator,
                                                  final StateSerdes<K, V> serdes) {
        this.cacheIter = cacheIter;
        this.storeIterator = storeIterator;
        this.store = store;
        this.serdes = serdes;
    }

    @Override
    public boolean hasNext() {
        final boolean storeHasNext = storeIterator.hasNext();
        return cacheIter.hasNext() || storeHasNext;
    }


    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        byte[] nextCacheKey = null;
        if (cacheIter.hasNext()) {
            nextCacheKey = cacheIter.peekNextKey();
        }

        byte[] nextStoreKey = null;
        if (storeIterator.hasNext()) {
            nextStoreKey = storeIterator.peekNextKey().get();
        }

        if (nextCacheKey == null) {
            return nextStoreValue();
        }

        if (nextStoreKey == null) {
            return nextCacheValue();
        }

        final int comparison = comparator.compare(nextCacheKey, nextStoreKey);
        // Use the cached item as it will be at least as new as the stored item
        if (comparison == 0) {
            storeIterator.next();
        }

        if (comparison <= 0) {
            return nextCacheValue();
        }

        return nextStoreValue();

    }

    private KeyValue<K, V> nextCacheValue() {
        final KeyValue<byte[], MemoryLRUCacheBytesEntry> next = cacheIter.next();
        return KeyValue.pair(serdes.keyFrom(next.key), serdes.valueFrom(next.value.value));
    }

    private KeyValue<K, V> nextStoreValue() {
        final KeyValue<Bytes, byte[]> next = storeIterator.next();
        return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
    }

    @Override
    public void remove() {
        // do nothing
    }

    @Override
    public void close() {
        cacheIter.close();
        storeIterator.close();
    }
}
