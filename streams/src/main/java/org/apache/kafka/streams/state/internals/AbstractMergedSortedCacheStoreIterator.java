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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.NoSuchElementException;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <V>
 */
abstract class AbstractMergedSortedCacheStoreIterator<K, KS, V, VS> implements KeyValueIterator<K, V> {
    private final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
    private final KeyValueIterator<KS, VS> storeIterator;

    AbstractMergedSortedCacheStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                           final KeyValueIterator<KS, VS> storeIterator) {
        this.cacheIterator = cacheIterator;
        this.storeIterator = storeIterator;
    }

    abstract int compare(final Bytes cacheKey, final KS storeKey);

    abstract K deserializeStoreKey(final KS key);

    abstract KeyValue<K, V> deserializeStorePair(final KeyValue<KS, VS> pair);

    abstract K deserializeCacheKey(final Bytes cacheKey);

    abstract V deserializeCacheValue(final LRUCacheEntry cacheEntry);

    private boolean isDeletedCacheEntry(final KeyValue<Bytes, LRUCacheEntry> nextFromCache) {
        return nextFromCache.value.value() == null;
    }

    @Override
    public boolean hasNext() {
        // skip over items deleted from cache, and corresponding store items if they have the same key
        while (cacheIterator.hasNext() && isDeletedCacheEntry(cacheIterator.peekNext())) {
            if (storeIterator.hasNext()) {
                final KS nextStoreKey = storeIterator.peekNextKey();
                // advance the store iterator if the key is the same as the deleted cache key
                if (compare(cacheIterator.peekNextKey(), nextStoreKey) == 0) {
                    storeIterator.next();
                }
            }
            cacheIterator.next();
        }

        return cacheIterator.hasNext() || storeIterator.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final Bytes nextCacheKey = cacheIterator.hasNext() ? cacheIterator.peekNextKey() : null;
        final KS nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

        if (nextCacheKey == null) {
            return nextStoreValue(nextStoreKey);
        }

        if (nextStoreKey == null) {
            return nextCacheValue(nextCacheKey);
        }

        final int comparison = compare(nextCacheKey, nextStoreKey);
        if (comparison > 0) {
            return nextStoreValue(nextStoreKey);
        } else if (comparison < 0) {
            return nextCacheValue(nextCacheKey);
        } else {
            // skip the same keyed element
            storeIterator.next();
            return nextCacheValue(nextCacheKey);
        }
    }

    private KeyValue<K, V> nextStoreValue(KS nextStoreKey) {
        final KeyValue<KS, VS> next = storeIterator.next();

        if (!next.key.equals(nextStoreKey)) {
            throw new IllegalStateException("Next record key is not the peeked key value; this should not happen");
        }

        return deserializeStorePair(next);
    }

    private KeyValue<K, V> nextCacheValue(Bytes nextCacheKey) {
        final KeyValue<Bytes, LRUCacheEntry> next = cacheIterator.next();

        if (!next.key.equals(nextCacheKey)) {
            throw new IllegalStateException("Next record key is not the peeked key value; this should not happen");
        }

        return KeyValue.pair(deserializeCacheKey(next.key), deserializeCacheValue(next.value));
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final Bytes nextCacheKey = cacheIterator.hasNext() ? cacheIterator.peekNextKey() : null;
        final KS nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

        if (nextCacheKey == null) {
            return deserializeStoreKey(nextStoreKey);
        }

        if (nextStoreKey == null) {
            return deserializeCacheKey(nextCacheKey);
        }

        final int comparison = compare(nextCacheKey, nextStoreKey);
        if (comparison > 0) {
            return deserializeStoreKey(nextStoreKey);
        } else if (comparison < 0) {
            return deserializeCacheKey(nextCacheKey);
        } else {
            // skip the same keyed element
            storeIterator.next();
            return deserializeCacheKey(nextCacheKey);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
    }

    @Override
    public void close() {
        cacheIterator.close();
        storeIterator.close();
    }
}

