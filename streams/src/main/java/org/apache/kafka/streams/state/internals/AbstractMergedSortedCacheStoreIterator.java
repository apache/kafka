package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.NoSuchElementException;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <V>
 */
abstract class AbstractMergedSortedCacheStoreIterator<K, KS, V> implements KeyValueIterator<K, V> {
    private final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
    private final KeyValueIterator<KS, byte[]> storeIterator;
    protected final StateSerdes<K, V> serdes;

    private KS nextStoreKey;
    private Bytes nextCacheKey;

    AbstractMergedSortedCacheStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                           final KeyValueIterator<KS, byte[]> storeIterator,
                                           final StateSerdes<K, V> serdes) {
        this.cacheIterator = cacheIterator;
        this.storeIterator = storeIterator;
        this.serdes = serdes;
    }

    abstract int compare(Bytes cacheKey, KS storeKey);

    abstract K deserializeStoreKey(KS key);

    abstract KeyValue<K, V> deserializeStorePair(KeyValue<KS, byte[]> pair);

    private boolean isDeletedCacheEntry(final KeyValue<Bytes, LRUCacheEntry> nextFromCache) {
        return nextFromCache.value.value == null;
    }

    @Override
    public boolean hasNext() {
        // skip over items deleted from cache, and corresponding store items if they have the same key
        while (cacheIterator.hasNext() && isDeletedCacheEntry(cacheIterator.peekNext())) {
            if (storeIterator.hasNext()) {
                nextStoreKey = storeIterator.peekNextKey();
                // advance the store iterator if the key is the same as the deleted cache key
                if (compare(cacheIterator.peekNextKey(), nextStoreKey) == 0) {
                    storeIterator.next();
                }
            }
            cacheIterator.next();
        }

        if (nextStoreKey == null && storeIterator.hasNext()) {
            nextStoreKey = storeIterator.peekNextKey();
        }

        if (nextCacheKey == null && cacheIterator.hasNext()) {
            nextCacheKey = cacheIterator.peekNextKey();
        }

        return nextStoreKey != null || nextCacheKey != null;
    }

    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

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
        final KeyValue<KS, byte[]> next = storeIterator.next();

        if (!next.key.equals(nextStoreKey))
            throw new IllegalStateException("Next record key is not the peeked key value; this should not happen");

        return deserializeStorePair(next);
    }

    private KeyValue<K, V> nextCacheValue(Bytes nextCacheKey) {
        final KeyValue<Bytes, LRUCacheEntry> next = cacheIterator.next();

        if (!next.key.equals(nextCacheKey))
            throw new IllegalStateException("Next record key is not the peeked key value; this should not happen");

        return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value.value));
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (nextCacheKey == null) {
            return deserializeStoreKey(nextStoreKey);
        }

        if (nextStoreKey == null) {
            return serdes.keyFrom(nextCacheKey.get());
        }

        final int comparison = compare(nextCacheKey, nextStoreKey);
        if (comparison > 0) {
            return deserializeStoreKey(nextStoreKey);
        } else if (comparison < 0) {
            return serdes.keyFrom(nextCacheKey.get());
        } else {
            // skip the same keyed element
            storeIterator.next();
            return serdes.keyFrom(nextCacheKey.get());
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported");
    }

    @Override
    public void close() {
        cacheIterator.close();
        storeIterator.close();
    }
}

