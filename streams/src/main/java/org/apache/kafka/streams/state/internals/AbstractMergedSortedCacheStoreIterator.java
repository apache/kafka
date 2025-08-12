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
 * AbstractMergedSortedCacheStoreIterator is an abstract class for merging two sorted iterators, one from a cache and
 * the other from a store. It ensures the merged results maintain sorted order while resolving conflicts between cache
 * and store entries.
 *
 * <p>This iterator is used for state stores in Kafka Streams, which have an (optional) caching layer that needs to be
 * "merged" with the underlying state. It handles common scenarios like skipping records with cached tombstones (deleted
 * entries) and preferring cache entries over store entries when conflicts arise.</p>
 *
 * @param <K>  The type of the resulting merged key.
 * @param <KS> The type of the store key.
 * @param <V>  The type of the resulting merged value.
 * @param <VS> The type of the store value.
 */
abstract class AbstractMergedSortedCacheStoreIterator<K, KS, V, VS> implements KeyValueIterator<K, V> {
    private final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
    private final KeyValueIterator<KS, VS> storeIterator;
    private final boolean forward;

    /**
     * Constructs an AbstractMergedSortedCacheStoreIterator.
     *
     * @param cacheIterator The iterator for the cache, assumed to be sorted by key.
     * @param storeIterator The iterator for the store, assumed to be sorted by key.
     * @param forward       The direction of iteration. True for forward, false for reverse.
     */
    AbstractMergedSortedCacheStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                           final KeyValueIterator<KS, VS> storeIterator,
                                           final boolean forward) {
        this.cacheIterator = cacheIterator;
        this.storeIterator = storeIterator;
        this.forward = forward;
    }

    /**
     * Compares the keys from the cache and store to determine their ordering.
     *
     * @param cacheKey The key from the cache.
     * @param storeKey The key from the store.
     *
     * @return A negative integer, zero, or a positive integer as the cache key is less than,
     *     equal to, or greater than the store key.
     */
    abstract int compare(final Bytes cacheKey, final KS storeKey);

    /**
     * Deserializes a store key into a generic merged key type.
     *
     * @param key The store key to deserialize.
     *
     * @return The deserialized key.
     */
    abstract K deserializeStoreKey(final KS key);

    /**
     * Deserializes a key-value pair from the store into a generic merged key-value pair.
     *
     * @param pair The key-value pair from the store.
     *
     * @return The deserialized key-value pair.
     */
    abstract KeyValue<K, V> deserializeStorePair(final KeyValue<KS, VS> pair);

    /**
     * Deserializes a cache key into a generic merged key type.
     *
     * @param cacheKey The cache key to deserialize.
     *
     * @return The deserialized key.
     */
    abstract K deserializeCacheKey(final Bytes cacheKey);

    /**
     * Deserializes a cache entry into a generic value type.
     *
     * @param cacheEntry The cache entry to deserialize.
     *
     * @return The deserialized value.
     */
    abstract V deserializeCacheValue(final LRUCacheEntry cacheEntry);

    /**
     * Checks if a cache entry is a tombstone (representing a deleted value).
     *
     * @param nextFromCache The cache entry to check.
     *
     * @return True if the cache entry is a tombstone, false otherwise.
     */
    private boolean isDeletedCacheEntry(final KeyValue<Bytes, LRUCacheEntry> nextFromCache) {
        return nextFromCache.value.value() == null;
    }

    /**
     * Determines if there are more entries to iterate over, resolving conflicts between cache and store entries (e.g.,
     * skipping tombstones).
     *
     * <p>Conflict resolution scenarios:</p>
     *
     * <ul>
     * <li><b>Cache contains a tombstone for a key:</b> Skip both the cache tombstone and the corresponding store entry (if exists).</li>
     * <li><b>Cache contains a value for a key present in the store:</b> Prefer the cache value and skip the store entry.</li>
     * <li><b>Cache key is unique:</b> Return the cache value.</li>
     * <li><b>Store key is unique:</b> Return the store value.</li>
     * </ul>
     *
     * @return True if there are more entries, false otherwise.
     */
    @Override
    public boolean hasNext() {
        // skip over items deleted from cache, and corresponding store items if they have the same key
        while (cacheIterator.hasNext() && isDeletedCacheEntry(cacheIterator.peekNext())) {
            if (!storeIterator.hasNext()) {
                // if storeIterator is exhausted, we can just skip over every tombstone
                // in the cache since they don't shadow any valid key
                cacheIterator.next();
                continue;
            }

            final KS nextStoreKey = storeIterator.peekNextKey();
            final int compare = compare(cacheIterator.peekNextKey(), nextStoreKey);

            if (compare == 0) {
                // next cache entry is a valid tombstone for the next store key
                storeIterator.next();
                cacheIterator.next();
            } else if (compare < 0) {
                // cache has a tombstone for an entry that doesn't exist in the store
                cacheIterator.next();
            } else {
                // store iterator has a valid entry, but we should not advance the cache
                // iterator because it may still shadow a future store key
                return true;
            }
        }

        return cacheIterator.hasNext() || storeIterator.hasNext();
    }

    /**
     * Retrieves the next key-value pair in the merged iteration.
     *
     * @return The next key-value pair.
     *
     * @throws NoSuchElementException If there are no more elements to iterate.
     */
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
        return chooseNextValue(nextCacheKey, nextStoreKey, comparison);
    }

    /**
     * Resolves which source (cache or store) to fetch the next key-value pair when a comparison is performed.
     *
     * @param nextCacheKey The next key from the cache.
     * @param nextStoreKey The next key from the store.
     * @param comparison   The comparison result between the cache and store keys.
     *
     * @return The next key-value pair.
     */
    private KeyValue<K, V> chooseNextValue(final Bytes nextCacheKey,
                                           final KS nextStoreKey,
                                           final int comparison) {
        if (forward) {
            if (comparison > 0) {
                return nextStoreValue(nextStoreKey);
            } else if (comparison < 0) {
                return nextCacheValue(nextCacheKey);
            } else {
                // skip the same keyed element
                storeIterator.next();
                return nextCacheValue(nextCacheKey);
            }
        } else {
            if (comparison < 0) {
                return nextStoreValue(nextStoreKey);
            } else if (comparison > 0) {
                return nextCacheValue(nextCacheKey);
            } else {
                // skip the same keyed element
                storeIterator.next();
                return nextCacheValue(nextCacheKey);
            }
        }
    }

    /**
     * Fetches the next value from the store, ensuring it matches the expected key.
     *
     * @param nextStoreKey The expected next key from the store.
     *
     * @return The next key-value pair from the store.
     *
     * @throws IllegalStateException If the key does not match the expected key.
     */
    private KeyValue<K, V> nextStoreValue(final KS nextStoreKey) {
        final KeyValue<KS, VS> next = storeIterator.next();

        if (!next.key.equals(nextStoreKey)) {
            throw new IllegalStateException("Next record key is not the peeked key value; this should not happen");
        }

        return deserializeStorePair(next);
    }

    /**
     * Fetches the next value from the cache, ensuring it matches the expected key.
     *
     * @param nextCacheKey The expected next key from the cache.
     *
     * @return The next key-value pair from the cache.
     *
     * @throws IllegalStateException If the key does not match the expected key.
     */
    private KeyValue<K, V> nextCacheValue(final Bytes nextCacheKey) {
        final KeyValue<Bytes, LRUCacheEntry> next = cacheIterator.next();

        if (!next.key.equals(nextCacheKey)) {
            throw new IllegalStateException("Next record key is not the peeked key value; this should not happen");
        }

        return KeyValue.pair(deserializeCacheKey(next.key), deserializeCacheValue(next.value));
    }

    /**
     * Peeks at the next key in the merged iteration without advancing the iterator.
     *
     * @return The next key in the iteration.
     *
     * @throws NoSuchElementException If there are no more elements to peek.
     */
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
        return chooseNextKey(nextCacheKey, nextStoreKey, comparison);
    }

    /**
     * Determines the next key to return from the merged iteration based on the comparison of the cache and store keys.
     * Resolves conflicts by considering the iteration direction and ensuring the merged order is maintained.
     *
     * @param nextCacheKey The next key from the cache.
     * @param nextStoreKey The next key from the store.
     * @param comparison   The comparison result between the cache and store keys. A negative value indicates the cache
     *                     key is smaller, zero indicates equality, and a positive value indicates the store key is
     *                     smaller.
     *
     * @return The next key to return from the merged iteration.
     */
    private K chooseNextKey(final Bytes nextCacheKey,
                            final KS nextStoreKey,
                            final int comparison) {
        if (forward) {
            if (comparison > 0) {
                return deserializeStoreKey(nextStoreKey);
            } else if (comparison < 0) {
                return deserializeCacheKey(nextCacheKey);
            } else {
                // skip the same keyed element
                storeIterator.next();
                return deserializeCacheKey(nextCacheKey);
            }
        } else {
            if (comparison < 0) {
                return deserializeStoreKey(nextStoreKey);
            } else if (comparison > 0) {
                return deserializeCacheKey(nextCacheKey);
            } else {
                // skip the same keyed element
                storeIterator.next();
                return deserializeCacheKey(nextCacheKey);
            }
        }
    }

    /**
     * Closes the iterators and releases any associated resources.
     */
    @Override
    public void close() {
        cacheIterator.close();
        storeIterator.close();
    }
}
