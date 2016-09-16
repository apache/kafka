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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <V>
 */
class MergedSortedCachedWindowStoreIterator<K, V> implements WindowStoreIterator<V> {
    private final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator;
    private final PeekingWindowIterator<byte[]> storeIterator;
    private final StateSerdes<K, V> serdes;

    public MergedSortedCachedWindowStoreIterator(final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator,
                                                 final PeekingWindowIterator<byte[]> storeIterator,
                                                 final StateSerdes<K, V> serdes) {
        this.cacheIterator = cacheIterator;
        this.storeIterator = storeIterator;
        this.serdes = serdes;
    }

    @Override
    public boolean hasNext() {
        return cacheIterator.hasNext() || storeIterator.hasNext();
    }


    @Override
    public KeyValue<Long, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Long nextCacheTimestamp = null;
        if (cacheIterator.hasNext()) {
            nextCacheTimestamp = WindowStoreUtils.timestampFromBinaryKey(cacheIterator.peekNextKey());
        }

        Long nextStoreTimestamp = null;
        if (storeIterator.hasNext()) {
            nextStoreTimestamp = storeIterator.peekNext().key;
        }

        if (nextCacheTimestamp == null) {
            return nextStoreValue();
        }

        if (nextStoreTimestamp == null) {
            return nextCacheValue(nextCacheTimestamp);
        }

        final int comparison = nextCacheTimestamp.compareTo(nextStoreTimestamp);
        if (comparison > 0) {
            return nextStoreValue();
        } else if (comparison < 0) {
            return nextCacheValue(nextCacheTimestamp);
        } else {
            storeIterator.next();
            return nextCacheValue(nextCacheTimestamp);
        }
    }

    private KeyValue<Long, V> nextCacheValue(final Long timestamp) {
        final KeyValue<byte[], LRUCacheEntry> next = cacheIterator.next();
        return KeyValue.pair(timestamp, serdes.valueFrom(next.value.value));
    }

    private KeyValue<Long, V> nextStoreValue() {
        final KeyValue<Long, byte[]> next = storeIterator.next();
        return KeyValue.pair(next.key, serdes.valueFrom(next.value));
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
