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
    private final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
    private final KeyValueIterator<Bytes, byte[]> storeIterator;
    private final StateSerdes<K, V> serdes;
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

    public MergedSortedCacheKeyValueStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                                  final KeyValueIterator<Bytes, byte[]> storeIterator,
                                                  final StateSerdes<K, V> serdes) {
        this.cacheIterator = cacheIterator;
        this.storeIterator = storeIterator;
        this.serdes = serdes;
    }

    @Override
    public boolean hasNext() {
        while (cacheIterator.hasNext() && isDeletedCacheEntry(cacheIterator.peekNext())) {
            if (storeIterator.hasNext()) {
                final Bytes storeKey = storeIterator.peekNextKey();
                // advance the store iterator if the key is the same as the deleted cache key
                if (storeKey.equals(cacheIterator.peekNextKey())) {
                    storeIterator.next();
                }
            }
            // skip over items deleted from cache
            cacheIterator.next();
        }
        return cacheIterator.hasNext() || storeIterator.hasNext();
    }


    private boolean isDeletedCacheEntry(final KeyValue<Bytes, LRUCacheEntry> nextFromCache) {
        return  nextFromCache.value.value == null;
    }


    @Override
    public KeyValue<K, V> next() {

        return internalNext(new NextValueFunction<KeyValue<K, V>>() {
            @Override
            public KeyValue<K, V> apply(final byte[] cacheKey, final byte[] storeKey) {
                if (cacheKey == null) {
                    return nextStoreValue();
                }

                if (storeKey == null) {
                    return nextCacheValue();
                }

                final int comparison = comparator.compare(cacheKey, storeKey);
                if (comparison > 0) {
                    return nextStoreValue();
                } else if (comparison < 0) {
                    return nextCacheValue();
                } else {
                    storeIterator.next();
                    return nextCacheValue();
                }
            }
        });
    }

    @Override
    public K peekNextKey() {
        return internalNext(new NextValueFunction<K>() {
            @Override
            public K apply(final byte[] cacheKey, final byte[] storeKey) {
                if (cacheKey == null) {
                    return serdes.keyFrom(storeKey);
                }

                if (storeKey == null) {
                    return serdes.keyFrom(cacheKey);
                }

                final int comparison = comparator.compare(cacheKey, storeKey);
                if (comparison > 0) {
                    return serdes.keyFrom(storeKey);
                } else {
                    return serdes.keyFrom(cacheKey);
                }
            }
        });
    }

    interface NextValueFunction<T> {
        T apply(final byte[] cacheKey, final byte [] storeKey);
    }

    private <T> T internalNext(final NextValueFunction<T> nextValueFunction) {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        byte[] nextCacheKey = null;
        if (cacheIterator.hasNext()) {
            nextCacheKey = cacheIterator.peekNextKey().get();
        }

        byte[] nextStoreKey = null;
        if (storeIterator.hasNext()) {
            nextStoreKey = storeIterator.peekNextKey().get();
        }

        return nextValueFunction.apply(nextCacheKey, nextStoreKey);
    }

    private KeyValue<K, V> nextCacheValue() {
        final KeyValue<Bytes, LRUCacheEntry> next = cacheIterator.next();
        return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value.value));
    }

    private KeyValue<K, V> nextStoreValue() {
        final KeyValue<Bytes, byte[]> next = storeIterator.next();
        return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

    @Override
    public void close() {
        cacheIterator.close();
        storeIterator.close();
    }
}
